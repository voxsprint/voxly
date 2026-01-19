const escapeMarkdown = (input = '') => {
    if (input === null || input === undefined) return '';
    return String(input)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
};

const buildLine = (emoji, label, value) => `${emoji} ${label}: ${value}`;

const section = (title, entries = []) => {
    const body = entries.filter(Boolean).join('\n');
    return `*${title}*\n${body}`;
};

const emphasize = (text = '') => {
    if (!text) return '';
    return `✨ ${text}`;
};

const tipLine = (emoji, text = '') => `${emoji} ${text}`;

module.exports = {
    escapeMarkdown,
    buildLine,
    section,
    emphasize,
    tipLine
};
