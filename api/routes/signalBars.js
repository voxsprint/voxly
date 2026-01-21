const DEFAULT_GLYPHS = ['▂', '▃', '▄', '▅', '▆'];

function renderSignalBars(strength, max = 5, empty = '░') {
  const maxBars = Number.isFinite(max) ? max : 5;
  const safeStrength = Number.isFinite(strength) ? strength : 0;
  const filled = Math.max(0, Math.min(maxBars, Math.round(safeStrength)));
  return DEFAULT_GLYPHS
    .slice(0, maxBars)
    .map((glyph, index) => (index < filled ? glyph : empty))
    .join('');
}

module.exports = {
  renderSignalBars
};
