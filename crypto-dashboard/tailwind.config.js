/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        slateInk: '#0f172a',
        panel: '#1e293b',
        panelSoft: '#263449',
        cyanLive: '#22d3ee',
        good: '#22c55e',
        bad: '#ef4444',
      },
      boxShadow: {
        glow: '0 0 28px rgba(34, 211, 238, 0.16)',
      },
      fontFamily: {
        mono: ['Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
};
