function normalizeReply(text, options = {}) {
    const normalizedText = text === undefined || text === null ? '' : String(text);
    const normalizedOptions = { ...options };

    if (!normalizedOptions.parse_mode) {
        if (/<[^>]+>/.test(normalizedText)) {
            normalizedOptions.parse_mode = 'HTML';
        } else if (/[`*_]/.test(normalizedText)) {
            normalizedOptions.parse_mode = 'Markdown';
        }
    }

    return { text: normalizedText, options: normalizedOptions };
}

function logCommandError(ctx, error) {
    const command = ctx.session?.lastCommand || ctx.message?.text || ctx.callbackQuery?.data || 'unknown';
    const userId = ctx.from?.id || 'unknown';
    const username = ctx.from?.username || 'unknown';
    const message = error?.message || error;
    console.error(`Command error (${command}) for user ${username} (${userId}):`, message);
}

function escapeHtml(text = '') {
    if (text === null || text === undefined) return '';
    return String(text)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function escapeMarkdown(text = '') {
    return String(text).replace(/([_*[\]()`])/g, '\\$1');
}

function emphasize(text = '') {
    return `*${text}*`;
}

function buildLine(icon, label, value) {
    const safeLabel = label ? escapeMarkdown(label) : '';
    const safeValue = value === undefined || value === null ? '' : String(value);
    return `${icon} ${safeLabel ? `*${safeLabel}:* ` : ''}${safeValue}`;
}

function tipLine(icon, text) {
    return `${icon} ${text}`;
}

function section(title, lines = []) {
    const body = Array.isArray(lines) ? lines : [lines];
    const cleaned = body.filter(Boolean);
    const header = emphasize(title);
    if (!cleaned.length) {
        return header;
    }
    return `${header}\n${cleaned.join('\n')}`;
}

async function styledAlert(ctx, message, options = {}) {
    return ctx.reply(section('⚠️ Notice', [message]), { parse_mode: 'Markdown', ...options });
}

module.exports = {
    normalizeReply,
    logCommandError,
    escapeHtml,
    escapeMarkdown,
    emphasize,
    buildLine,
    tipLine,
    section,
    styledAlert
};
