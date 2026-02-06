import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react-swc';
import tsconfigPaths from 'vite-tsconfig-paths';

// https://vitejs.dev/config/
export default defineConfig({
  base: process.env.VITE_BASE || '/',
  plugins: [react(), tsconfigPaths()],
  server: {
    host: true,
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
    rollupOptions: {
      output: {
        manualChunks: {
          'vendor-telegram': ['@telegram-apps/telegram-ui', '@tma.js/sdk-react'],
          'vendor-tonconnect': ['@tonconnect/ui-react'],
          'vendor-core': ['react', 'react-dom', 'react-router-dom'],
        },
      },
    },
    minify: 'terser',
    terserOptions: {
      compress: {
        drop_console: true,
        drop_debugger: true,
      },
      mangle: true,
    },
  },
  define: {
    __DEV__: JSON.stringify(process.env.MODE !== 'production'),
  },
});
