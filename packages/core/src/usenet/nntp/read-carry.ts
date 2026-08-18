const KIBIBYTE_BYTES = 1024;

/** Size of the reusable transport window supplied to Node's `onread` hook. */
export const NNTP_READ_WINDOW_BYTES = 256 * KIBIBYTE_BYTES;

/**
 * Hard ownership window for bytes delivered after local socket backpressure.
 *
 * TLS may have already decrypted callbacks when `socket.pause()` linearizes.
 * Four transport windows cover the unconsumed current window plus three full
 * late callbacks. The matching bytes are reserved before a BODY goes on wire;
 * the queue itself remains lazy and allocates only for callbacks that arrive.
 */
export const NNTP_READ_CARRY_MAX_BYTES = 4 * NNTP_READ_WINDOW_BYTES;

/**
 * TLS records can surface as many smaller callbacks rather than four full
 * transport windows. Two hundred fifty-six entries cover the tested 4 KiB
 * fragmentation across the complete 1 MiB byte window while keeping callback
 * metadata strictly bounded independently of payload bytes.
 */
export const NNTP_READ_CARRY_MAX_CHUNKS = 256;
