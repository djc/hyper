use http::header::{
    HeaderName, CONNECTION, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, TE, TRAILER,
    TRANSFER_ENCODING, UPGRADE,
};
use http::HeaderMap;

use super::DecodedLength;
use crate::headers::content_length_parse_all;

pub(crate) mod server;

pub(crate) use self::server::Server;

fn strip_connection_headers(headers: &mut HeaderMap, is_request: bool) {
    // List of connection headers from:
    // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Connection
    //
    // TE headers are allowed in HTTP/2 requests as long as the value is "trailers", so they're
    // tested separately.
    let connection_headers = [
        HeaderName::from_lowercase(b"keep-alive").unwrap(),
        HeaderName::from_lowercase(b"proxy-connection").unwrap(),
        PROXY_AUTHENTICATE,
        PROXY_AUTHORIZATION,
        TRAILER,
        TRANSFER_ENCODING,
        UPGRADE,
    ];

    for header in connection_headers.iter() {
        if headers.remove(header).is_some() {
            warn!("Connection header illegal in HTTP/2: {}", header.as_str());
        }
    }

    if is_request {
        if headers
            .get(TE)
            .map(|te_header| te_header != "trailers")
            .unwrap_or(false)
        {
            warn!("TE headers not set to \"trailers\" are illegal in HTTP/2 requests");
            headers.remove(TE);
        }
    } else if headers.remove(TE).is_some() {
        warn!("TE headers illegal in HTTP/2 responses");
    }

    if let Some(header) = headers.remove(CONNECTION) {
        warn!(
            "Connection header illegal in HTTP/2: {}",
            CONNECTION.as_str()
        );
        let header_contents = header.to_str().unwrap();

        // A `Connection` header may have a comma-separated list of names of other headers that
        // are meant for only this specific connection.
        //
        // Iterate these names and remove them as headers. Connection-specific headers are
        // forbidden in HTTP2, as that information has been moved into frame types of the h2
        // protocol.
        for name in header_contents.split(',') {
            let name = name.trim();
            headers.remove(name);
        }
    }
}

fn decode_content_length(headers: &HeaderMap) -> DecodedLength {
    if let Some(len) = content_length_parse_all(headers) {
        // If the length is u64::MAX, oh well, just reported chunked.
        DecodedLength::checked_new(len).unwrap_or_else(|_| DecodedLength::CHUNKED)
    } else {
        DecodedLength::CHUNKED
    }
}
