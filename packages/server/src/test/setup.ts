process.env.NODE_ENV ??= 'test';
process.env.SECRET_KEY ??= '0'.repeat(64);
if (!process.env.BASE_URL || process.env.BASE_URL === '/') {
  process.env.BASE_URL = 'http://localhost:3000';
}
process.env.LOG_LEVEL ??= 'error';
process.env.AIOSTREAMS_AUTH ??= 'proxy-shutdown-user:proxy-shutdown-password';
