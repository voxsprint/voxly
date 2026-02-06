import { useCallback, useEffect, useState } from 'react';
import {
  Banner,
  Button,
  Input,
  List,
  Section,
  Select,
  Textarea,
} from '@telegram-apps/telegram-ui';
import { apiFetch, createIdempotencyKey } from '../lib/api';
import { confirmAction, hapticSuccess, hapticError } from '../lib/ux';
import { trackEvent } from '../lib/telemetry';
import { useUser } from '../state/user';

type CallerFlag = {
  id: number;
  phone_number: string;
  label: string;
  description?: string | null;
  action?: 'allow' | 'block' | 'route' | 'tag';
  route_script_id?: number | null;
  created_at: string;
  updated_by?: string | null;
};

type CallerFlagResponse = {
  ok: boolean;
  flags: CallerFlag[];
};

type SaveFlagResponse = {
  ok: boolean;
  flag?: CallerFlag;
  error?: string;
};

export function CallerFlags() {
  const { roles } = useUser();
  const isAdmin = roles.includes('admin');

  const [flags, setFlags] = useState<CallerFlag[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [showForm, setShowForm] = useState(false);

  // Form state
  const [phoneNumber, setPhoneNumber] = useState('');
  const [label, setLabel] = useState('');
  const [description, setDescription] = useState('');
  const [action, setAction] = useState<'allow' | 'block' | 'route' | 'tag'>('tag');
  const [routeScriptId, setRouteScriptId] = useState('');

  const loadFlags = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (searchQuery) params.set('q', searchQuery);
      params.set('limit', '50');
      const response = await apiFetch<CallerFlagResponse>(
        `/webapp/caller-flags?${params.toString()}`
      );
      setFlags(response.flags || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load caller flags');
      trackEvent('caller_flags_load_failed');
    } finally {
      setLoading(false);
    }
  }, [searchQuery]);

  useEffect(() => {
    loadFlags();
  }, [loadFlags]);

  const handleAddFlag = async () => {
    if (!phoneNumber.trim()) {
      setError('Phone number is required');
      return;
    }

    if (!label.trim()) {
      setError('Label is required');
      return;
    }

    setSaving(true);
    setError(null);
    setSuccess(null);

    try {
      const response = await apiFetch<SaveFlagResponse>('/webapp/caller-flags', {
        method: 'POST',
        body: {
          phone_number: phoneNumber.trim(),
          label: label.trim(),
          description: description.trim() || null,
          action,
          route_script_id: routeScriptId ? parseInt(routeScriptId, 10) : null,
        },
        idempotencyKey: createIdempotencyKey(),
      });

      if (response.ok) {
        hapticSuccess();
        setSuccess(`Flag added for ${phoneNumber}`);
        setPhoneNumber('');
        setLabel('');
        setDescription('');
        setAction('tag');
        setRouteScriptId('');
        setShowForm(false);
        trackEvent('caller_flag_added', { action });
        await loadFlags();
      } else {
        throw new Error(response.error || 'Failed to add flag');
      }
    } catch (err) {
      hapticError();
      setError(err instanceof Error ? err.message : 'Failed to add flag');
      trackEvent('caller_flag_add_failed');
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteFlag = async (flagId: number, phone: string) => {
    if (!isAdmin) return;

    const confirmed = await confirmAction({
      title: 'Delete Flag?',
      message: `Remove the flag for ${phone}?`,
      confirmText: 'Delete',
      destructive: true,
    });

    if (!confirmed) return;

    setSaving(true);
    setError(null);

    try {
      await apiFetch(`/webapp/caller-flags/${flagId}`, {
        method: 'DELETE',
        idempotencyKey: createIdempotencyKey(),
      });

      hapticSuccess();
      setSuccess(`Flag deleted for ${phone}`);
      trackEvent('caller_flag_deleted', { flag_id: flagId });
      await loadFlags();
    } catch (err) {
      hapticError();
      setError(err instanceof Error ? err.message : 'Failed to delete flag');
      trackEvent('caller_flag_delete_failed', { flag_id: flagId });
    } finally {
      setSaving(false);
    }
  };

  const filteredFlags = flags.filter((flag) => {
    const q = searchQuery.toLowerCase();
    return (
      flag.phone_number.includes(q) ||
      flag.label.toLowerCase().includes(q) ||
      (flag.description && flag.description.toLowerCase().includes(q))
    );
  });

  const getActionBadgeColor = (action?: string) => {
    switch (action) {
      case 'allow':
        return 'success';
      case 'block':
        return 'error';
      case 'route':
        return 'primary';
      case 'tag':
      default:
        return '';
    }
  };

  return (
    <div className="wallet-page">
      <List className="wallet-list">
        {error && <Banner type="inline" header="Error" description={error} />}
        {success && <Banner type="inline" header="Success" description={success} />}

        <Section header="Caller Flags" className="wallet-section">
          <div className="card-header">
            <span>Manage Inbound Callers</span>
            {isAdmin && (
              <Button
                size="s"
                mode="bezeled"
                onClick={() => setShowForm(!showForm)}
              >
                {showForm ? 'Cancel' : 'Add Flag'}
              </Button>
            )}
          </div>

          {showForm && isAdmin && (
            <div className="form-section">
              <Input
                header="Phone Number"
                placeholder="+1234567890"
                value={phoneNumber}
                onChange={(e) => setPhoneNumber(e.target.value)}
                disabled={saving}
                type="tel"
              />

              <Input
                header="Label"
                placeholder="e.g., VIP Customer, Support"
                value={label}
                onChange={(e) => setLabel(e.target.value)}
                disabled={saving}
              />

              <Select
                header="Action"
                value={action}
                onChange={(e) => setAction(e.target.value as any)}
                disabled={saving}
              >
                <option value="tag">Tag (identify)</option>
                <option value="allow">Allow (whitelist)</option>
                <option value="block">Block (blacklist)</option>
                <option value="route">Route (to specific script)</option>
              </Select>

              <Textarea
                header="Description (optional)"
                placeholder="Additional notes about this caller..."
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                disabled={saving}
              />

              <div className="section-actions">
                <Button
                  size="m"
                  mode="filled"
                  onClick={handleAddFlag}
                  disabled={saving || !phoneNumber.trim() || !label.trim()}
                >
                  {saving ? 'Adding...' : 'Add Flag'}
                </Button>
              </div>
            </div>
          )}

          <Input
            placeholder="Search by phone or label..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />

          <div className="card-section">
            <div className="card-header">
              <span>Caller Flags</span>
              <span className="card-header-muted">
                {loading ? 'Loading...' : `${filteredFlags.length} flags`}
              </span>
            </div>

            {filteredFlags.length === 0 ? (
              <div className="empty-card">
                <div className="empty-title">
                  {flags.length === 0 ? 'No caller flags' : 'No matching flags'}
                </div>
                <div className="empty-subtitle">
                  {flags.length === 0
                    ? 'Caller flags help identify and route inbound calls.'
                    : 'Try a different search query.'}
                </div>
              </div>
            ) : (
              <div className="card-list">
                {filteredFlags.map((flag) => (
                  <div key={flag.id} className="card-item">
                    <div className="card-item-main">
                      <div className="card-item-title">{flag.phone_number}</div>
                      <div className="card-item-subtitle">{flag.label}</div>
                      {flag.description && (
                        <div className="card-item-meta">{flag.description}</div>
                      )}
                      <div className="card-item-meta">
                        Added {new Date(flag.created_at).toLocaleDateString()}
                        {flag.updated_by && ` by ${flag.updated_by}`}
                      </div>
                    </div>
                    <div className="tag-group">
                      <span className={`tag ${getActionBadgeColor(flag.action)}`}>
                        {flag.action || 'tag'}
                      </span>
                      {isAdmin && (
                        <Button
                          size="s"
                          mode="plain"
                          onClick={() => handleDeleteFlag(flag.id, flag.phone_number)}
                          disabled={saving}
                          style={{ color: 'var(--tg-theme-destructive-text-color)' }}
                        >
                          Delete
                        </Button>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </Section>
      </List>
    </div>
  );
}
