use std::error::Error as StdError;
use std::marker::PhantomData;

use futures_util::stream::Stream;
use h3::{server::Sender, SendData};
use http_body::Body as HttpBody;
use pin_project::{pin_project, project};

use super::decode_content_length;

use crate::common::{exec::Executor, task, Future, Pin, Poll};
use crate::headers;
use crate::proto::{Dispatched, HttpStream};
use crate::service::HttpService;

use crate::{Body, Response};

// Our defaults are chosen for the "majority" case, which usually are not
// resource constrained, and so the spec default of 64kb can be too limiting
// for performance.
//
// At the same time, a server more often has multiple clients connected, and
// so is more likely to use more resources than a client would.
const DEFAULT_CONN_WINDOW: u32 = 1024 * 1024; // 1mb
const DEFAULT_STREAM_WINDOW: u32 = 1024 * 1024; // 1mb

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) adaptive_window: bool,
    pub(crate) initial_conn_window_size: u32,
    pub(crate) initial_stream_window_size: u32,
    pub(crate) max_concurrent_streams: Option<u32>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            adaptive_window: false,
            initial_conn_window_size: DEFAULT_CONN_WINDOW,
            initial_stream_window_size: DEFAULT_STREAM_WINDOW,
            max_concurrent_streams: None,
        }
    }
}

#[pin_project]
pub(crate) struct Server<S, B, E>
where
    S: HttpService<Body>,
{
    exec: E,
    service: S,
    state: State,
    _body: PhantomData<B>,
}

enum State {
    Connecting(h3::server::Connecting),
    Serving(Serving),
    Closed,
}

struct Serving {
    conn: h3::server::IncomingRequest,
    closing: Option<crate::Error>,
}

impl<S, B, E> Server<S, B, E>
where
    S: HttpService<Body, ResBody = B>,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    B: HttpBody,
    E: Executor<HttpStream<S::Future, B>>,
{
    pub(crate) fn new(
        io: quinn::Connecting,
        service: S,
        _config: &Config,
        exec: E,
    ) -> Server<S, B, E> {
        let io = h3::server::Connecting::from_quic(io, h3::Settings::default());
        Server {
            exec,
            state: State::Connecting(io),
            service,
            _body: PhantomData::default(),
        }
    }

    pub fn graceful_shutdown(&mut self) {
        trace!("graceful_shutdown");
        match &mut self.state {
            State::Connecting(_) => {
                // fall-through, to replace state with Closed
            }
            State::Serving(ref mut srv) => {
                if srv.closing.is_none() {
                    srv.conn.go_away();
                }
                return;
            }
            State::Closed => {
                return;
            }
        }
        self.state = State::Closed;
    }
}

impl<S, B, E> Future for Server<S, B, E>
where
    S: HttpService<Body, ResBody = B>,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    B: HttpBody,
    E: Executor<HttpStream<S::Future, B>>,
{
    type Output = crate::Result<Dispatched>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        loop {
            let next = match me.state {
                State::Connecting(ref mut hs) => {
                    let conn = ready!(Pin::new(hs).poll(cx).map_err(crate::Error::new_h3))?;
                    State::Serving(Serving {
                        conn,
                        closing: None,
                    })
                }
                State::Serving(ref mut conn) => {
                    ready!(conn.poll_server(cx, &mut me.service, &mut me.exec))?;
                    return Poll::Ready(Ok(Dispatched::Shutdown));
                }
                State::Closed => {
                    // graceful_shutdown was called before handshaking finished,
                    // nothing to do here...
                    return Poll::Ready(Ok(Dispatched::Shutdown));
                }
            };
            me.state = next;
        }
    }
}

impl Serving {
    fn poll_server<S, B, E>(
        &mut self,
        cx: &mut task::Context<'_>,
        service: &mut S,
        exec: &mut E,
    ) -> Poll<crate::Result<()>>
    where
        S: HttpService<Body>,
        S::Error: Into<Box<dyn StdError + Send + Sync>>,
        E: Executor<HttpStream<S::Future, B>>,
        B: HttpBody,
    {
        if let Some(err) = self.closing.take() {
            return Poll::Ready(Err(err));
        }

        loop {
            // Check that the service is ready to accept a new request.
            //
            // - If not, just drive the connection some.
            // - If ready, try to accept a new request from the connection.
            match service.poll_ready(cx) {
                Poll::Ready(Ok(())) => (),
                Poll::Pending => return Poll::Ready(Ok(())),
                Poll::Ready(Err(err)) => {
                    let err = crate::Error::new_user_service(err);
                    debug!("service closed: {}", err);

                    let error = err.h3_reason();
                    if let h3::HttpError::NoError = error {
                        // NO_ERROR is only used for graceful shutdowns...
                        trace!("interpreting NO_ERROR user error as graceful_shutdown");
                        self.conn.go_away();
                    } else {
                        // TODO: send proper error code?
                        self.conn.go_away();
                    }
                    return Poll::Ready(Err(err));
                }
            }

            // When the service is ready, accepts an incoming request.
            let mut req_receiver = match ready!(Pin::new(&mut self.conn).poll_next(cx)) {
                Some(req_receiver) => req_receiver,
                None => {
                    // no more incoming streams...
                    trace!("incoming connection complete");
                    return Poll::Ready(Ok(()));
                }
            };

            trace!("incoming request");
            match ready!(Pin::new(&mut req_receiver).poll(cx)) {
                Ok((req, respond)) => {
                    let content_length = decode_content_length(req.headers());
                    let req = req.map(move |stream| crate::Body::h3(stream, content_length));
                    let fut = H3Stream::new(service.call(req), respond);
                    exec.execute(HttpStream::H3(fut));
                }
                Err(e) => {
                    return Poll::Ready(Err(crate::Error::new_h3(e)));
                }
            }
        }
    }
}

#[allow(missing_debug_implementations)]
#[pin_project]
pub struct H3Stream<F, B>
where
    B: HttpBody,
{
    #[pin]
    state: H3StreamState<F, B>,
}

#[pin_project]
enum H3StreamState<F, B>
where
    B: HttpBody,
{
    Service(#[pin] Sender, #[pin] F),
    Body(#[pin] SendData<B, B::Data>),
}

impl<F, B> H3Stream<F, B>
where
    B: HttpBody,
{
    fn new(fut: F, respond: Sender) -> H3Stream<F, B> {
        H3Stream {
            state: H3StreamState::Service(respond, fut),
        }
    }
}

impl<F, B, E> H3Stream<F, B>
where
    F: Future<Output = Result<Response<B>, E>>,
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>> + Send + Sync,
    E: Into<Box<dyn StdError + Send + Sync>>,
{
    #[project]
    pub(crate) fn poll3(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> Poll<crate::Result<()>> {
        let mut me = self.project();
        loop {
            #[project]
            let next = match me.state.as_mut().project() {
                H3StreamState::Service(mut sender, svc) => {
                    let mut res = match svc.poll(cx) {
                        Poll::Ready(Ok(r)) => r,
                        Poll::Pending => {
                            // TODO: poll for client having canceled the request
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(e)) => {
                            let err = crate::Error::new_user_service(e);
                            warn!("http3 service errored: {}", err);
                            // TODO: send proper error code error
                            sender.cancel();
                            return Poll::Ready(Err(err));
                        }
                    };

                    super::strip_connection_headers(res.headers_mut(), false);

                    // set Date header if it isn't already set...
                    res.headers_mut()
                        .entry(::http::header::DATE)
                        .or_insert_with(crate::proto::h1::date::update_and_header_value);

                    // automatically set Content-Length from body...
                    if let Some(len) = res.body().size_hint().exact() {
                        headers::set_content_length_if_missing(res.headers_mut(), len);
                    }

                    H3StreamState::Body(sender.send_response(res))
                }
                H3StreamState::Body(sending) => {
                    return sending.poll(cx).map_err(|e| crate::Error::new_h3(e));
                }
            };
            me.state.set(next);
        }
    }
}
