/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        slate: {
          50: '#eaffff',
          100: '#c7fbff',
          200: '#9cebf0',
          300: '#79dfe7',
          400: '#4f9ea8',
          500: '#327781',
          600: '#245a61',
          700: '#155d67',
          800: '#062128',
          900: '#041216',
          950: '#02070a',
        },
        slateInk: '#02070a',
        panel: '#041216',
        panelSoft: '#062128',
        cyanLive: '#35e6f2',
        good: '#37ff7d',
        bad: '#ef4444',
      },
      boxShadow: {
        glow: '0 0 24px rgba(53, 230, 242, 0.18)',
      },
      fontFamily: {
        mono: ['Cascadia Mono', 'Consolas', 'Courier New', 'monospace'],
      },
    },
  },
  plugins: [],
};
