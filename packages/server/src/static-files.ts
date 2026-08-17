/** Static fallback videos served to playback clients for terminal outcomes. */
export enum StaticFiles {
  DOWNLOAD_FAILED = 'download_failed.mp4',
  DOWNLOADING = 'downloading.mp4',
  UNAVAILABLE_FOR_LEGAL_REASONS = 'unavailable_for_legal_reasons.mp4',
  STORE_LIMIT_EXCEEDED = 'store_limit_exceeded.mp4',
  CONTENT_PROXY_LIMIT_REACHED = 'content_proxy_limit_reached.mp4',
  INTERNAL_SERVER_ERROR = '500.mp4',
  TOO_MANY_REQUESTS = '429.mp4',
  FORBIDDEN = '403.mp4',
  UNAUTHORIZED = '401.mp4',
  NO_MATCHING_FILE = 'no_matching_file.mp4',
  PAYMENT_REQUIRED = 'payment_required.mp4',
  OK = '200.mp4',
}
