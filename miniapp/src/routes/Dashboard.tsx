import { useEffect, useMemo } from 'react';
import {
  Badge,
  Button,
  Cell,
  Chip,
  List,
  Placeholder,
  Section,
} from '@telegram-apps/telegram-ui';
import { useCalls } from '../state/calls';
import { navigate } from '../lib/router';

export function Dashboard() {
  const { calls, inboundQueue, fetchCalls, fetchInboundQueue, loading } = useCalls();

  useEffect(() => {
    fetchCalls({ limit: 10 });
    fetchInboundQueue();
  }, [fetchCalls, fetchInboundQueue]);

  const stats = useMemo(() => {
    const total = calls.length;
    const active = calls.filter((call) => ['in-progress', 'answered', 'ringing'].includes(String(call.status))).length;
    const completed = calls.filter((call) => String(call.status) === 'completed').length;
    return { total, active, completed };
  }, [calls]);

  return (
    <List>
      <Section header="Quick stats">
        <Cell
          subtitle="Total (recent)"
          after={<Badge type="number" mode="primary">{stats.total}</Badge>}
        >
          Total calls
        </Cell>
        <Cell
          subtitle="Active (ringing / answered)"
          after={<Badge type="number" mode="secondary">{stats.active}</Badge>}
        >
          Active calls
        </Cell>
        <Cell
          subtitle="Completed"
          after={<Badge type="number" mode="gray">{stats.completed}</Badge>}
        >
          Completed calls
        </Cell>
      </Section>

      <Section header="Inbound queue">
        {inboundQueue.length === 0 ? (
          <Placeholder
            header="No inbound calls"
            description="You're all caught up."
            action={(
              <Button size="s" mode="bezeled" onClick={() => navigate('/inbox')}>
                Open inbox
              </Button>
            )}
          />
        ) : (
          inboundQueue.slice(0, 3).map((call) => (
            <Cell
              key={call.call_sid}
              subtitle={call.route_label || call.script || 'Inbound'}
              description={call.from || 'Unknown caller'}
              after={<Chip mode="mono">{call.decision || 'pending'}</Chip>}
            >
              Inbound call
            </Cell>
          ))
        )}
      </Section>

      <Section
        header="Recent activity"
        footer={loading && calls.length === 0 ? 'Loading calls...' : undefined}
      >
        {calls.slice(0, 5).map((call) => (
          <Cell
            key={call.call_sid}
            subtitle={call.status || 'unknown'}
            description={call.created_at || '-'}
            after={<Chip mode="outline">{call.status || 'unknown'}</Chip>}
          >
            {call.phone_number || call.call_sid}
          </Cell>
        ))}
      </Section>
    </List>
  );
}
