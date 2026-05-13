import { resolve } from "path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": resolve(__dirname, "./src"),
    },
  },
  build: {
    // Raise warning threshold — echarts alone is ~600KB minified+gzipped in its own chunk
    chunkSizeWarningLimit: 700,
    rollupOptions: {
      output: {
        manualChunks(id) {
          // ECharts splits into its own async chunk — ~1.4MB → loaded only when a chart renders
          if (id.includes("node_modules/echarts") || id.includes("node_modules/echarts-for-react") || id.includes("node_modules/zrender")) {
            return "vendor-echarts";
          }
          // PDF libs split together — ~1.3MB → loaded only when Report tab opens
          if (
            id.includes("node_modules/@react-pdf") ||
            id.includes("node_modules/jspdf") ||
            id.includes("node_modules/@fontsource") ||
            id.includes("node_modules/fflate") ||
            id.includes("node_modules/canvg") ||
            id.includes("node_modules/html2canvas")
          ) {
            return "vendor-pdf";
          }
          // Framer Motion → separate async chunk
          if (id.includes("node_modules/framer-motion")) {
            return "vendor-motion";
          }
          // TanStack Query → separate async chunk
          if (id.includes("node_modules/@tanstack")) {
            return "vendor-query";
          }
          // Remaining node_modules → shared vendor chunk
          if (id.includes("node_modules")) {
            return "vendor";
          }
        },
      },
    },
  },
  server: {
    host: "0.0.0.0",
    port: 5173,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
    },
  },
});
