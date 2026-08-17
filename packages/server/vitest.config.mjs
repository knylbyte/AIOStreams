import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    setupFiles: ['./src/test/setup.ts'],
    server: {
      deps: {
        // Exercise the same built Core package that the compiled server loads.
        external: [/packages\/core\/dist\//],
      },
    },
  },
});
