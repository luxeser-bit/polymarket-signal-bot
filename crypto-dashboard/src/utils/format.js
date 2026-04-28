export function numberCompact(value, digits = 0) {
  const number = Number(value || 0);
  return new Intl.NumberFormat('en-US', {
    notation: Math.abs(number) >= 100000 ? 'compact' : 'standard',
    maximumFractionDigits: digits,
  }).format(number);
}

export function numberFull(value, digits = 0) {
  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits: digits,
  }).format(Number(value || 0));
}

export function money(value, digits = 2) {
  const number = Number(value || 0);
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: digits,
  }).format(number);
}

export function percent(value, digits = 2) {
  return `${(Number(value || 0) * 100).toFixed(digits)}%`;
}

export function signedMoney(value, digits = 2) {
  const number = Number(value || 0);
  return `${number >= 0 ? '+' : ''}${money(number, digits)}`;
}

export function shortAddress(value) {
  const text = String(value || '');
  if (text.length <= 14) return text || '-';
  return `${text.slice(0, 6)}...${text.slice(-4)}`;
}

export function secondsToDuration(value) {
  const seconds = Math.max(0, Number(value || 0));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

export function timestampLabel(value) {
  const ts = Number(value || 0);
  if (!ts) return '-';
  return new Date(ts * 1000).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

export function clamp(value, min = 0, max = 1) {
  return Math.min(max, Math.max(min, Number(value || 0)));
}
