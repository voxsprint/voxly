import { useEffect, useState } from 'react';
import {
  Button,
  Banner,
  Cell,
  InlineButtons,
  List,
  Placeholder,
  Section,
} from '@telegram-apps/telegram-ui';
import { apiFetch, createIdempotencyKey } from '../lib/api';
import { useCalls } from '../state/calls';
import { navigate } from '../lib/router';

export function Inbox() {
  const { inboundQueue, inboundNotice, fetchInboundQueue } = useCalls();
  const [busyCall, setBusyCall] = useState<string | null>(null);

  useEffect(() => {
    fetchInboundQueue();
    const timer = window.setInterval(fetchInboundQueue, 5000);
    return () => window.clearInterval(timer);
  }, [fetchInboundQueue]);

  const handleAction = async (callSid: string, action: 'answer' | 'decline') => {
    setBusyCall(callSid);
    try {
      await apiFetch(`/webapp/inbound/${callSid}/${action}`, {
        method: 'POST',
        idempotencyKey: createIdempotencyKey(),
      });
      if (action === 'answer') {
        navigate(`/calls/${callSid}`);
      }
      await fetchInboundQueue();
    } finally {
      setBusyCall(null);
    }
  };

  return (
    <List>
      {inboundNotice && (
        <Banner
          type="inline"
          header="Incoming call pending"
          description={inboundNotice.message}
        />
      )}
      <Section header="Inbound queue">
        {inboundQueue.length === 0 ? (
          <Placeholder
            header="No inbound calls"
            description="Calls will appear here when ringing."
            action={(
              <Button size="s" mode="bezeled" onClick={() => fetchInboundQueue()}>
                Refresh
              </Button>
            )}
          />
        ) : (
          inboundQueue.map((call) => {
            const disabled = busyCall === call.call_sid || call.decision === 'answered' || call.decision === 'declined';
            return (
              <Cell
                key={call.call_sid}
                subtitle={call.route_label || call.script || 'Inbound call'}
                description={call.from || 'Unknown caller'}
                after={(
                  <InlineButtons mode="bezeled">
                    <InlineButtons.Item
                      text="Answer"
                      disabled={disabled}
                      onClick={() => handleAction(call.call_sid, 'answer')}
                    />
                    <InlineButtons.Item
                      text="Decline"
                      disabled={disabled}
                      onClick={() => handleAction(call.call_sid, 'decline')}
                    />
                  </InlineButtons>
                )}
              >
                Inbound call
              </Cell>
            );
          })
        )}
      </Section>
    </List>
  );
}
