const DEFAULT_PARSE_MODE = 'HTML';

const escapeHtml = (input = '') => {
    if (input === null || input === undefined) return '';
    return String(input)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
};

const formatCommandMessage = (text = '') => {
    const safe = escapeHtml(text);
    return safe
        .replace(/\*([^*]+)\*/g, '<b>$1</b>')
        .replace(/`([^`]+)`/g, '<code>$1</code>');
};

const normalizeReply = (text, options = {}) => {
    if (options?.parse_mode === DEFAULT_PARSE_MODE) {
        return { text, options };
    }
    const formatted = formatCommandMessage(text);
    return {
        text: formatted,
        options: {
            ...options,
            parse_mode: DEFAULT_PARSE_MODE
        }
    };
};

const logCommandError = (ctx, error, extra = {}) => {
    const command = ctx?.message?.text?.split(' ')[0] || ctx?.callbackQuery?.data || 'unknown';
    const payload = {
        event: 'bot_command_error',
        command,
        user_id: ctx?.from?.id || null,
        username: ctx?.from?.username || null,
        chat_id: ctx?.chat?.id || null,
        message: error?.message || 'unknown_error',
        stack: error?.stack,
        ...extra
    };
    try {
        console.error(JSON.stringify(payload));
    } catch (_) {
        console.error('bot_command_error', payload);
    }
};

module.exports = {
    DEFAULT_PARSE_MODE,
    escapeHtml,
    formatCommandMessage,
    normalizeReply,
    logCommandError
};
