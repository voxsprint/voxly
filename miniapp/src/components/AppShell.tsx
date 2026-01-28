import { useEffect, useMemo, useState } from 'react';
import { backButton } from '@tma.js/sdk-react';
import {
  Badge,
  Banner,
  Button,
  Cell,
  List,
  Navigation,
  Section,
} from '@telegram-apps/telegram-ui';
import { matchRoute, navigate, getHashPath } from '../lib/router';
import { useUser } from '../state/user';
import { CallsProvider } from '../state/calls';
import { Dashboard } from '../routes/Dashboard';
import { Inbox } from '../routes/Inbox';
import { Calls } from '../routes/Calls';
import { CallConsole } from '../routes/CallConsole';
import { Scripts } from '../routes/Scripts';
import { Users } from '../routes/Users';
import { Settings } from '../routes/Settings';

type NavItem = {
  label: string;
  path: string;
  adminOnly?: boolean;
};

const navItems: NavItem[] = [
  { label: 'Dashboard', path: '/' },
  { label: 'Inbox', path: '/inbox' },
  { label: 'Calls', path: '/calls' },
  { label: 'Scripts', path: '/scripts' },
  { label: 'Users', path: '/users', adminOnly: true },
  { label: 'Settings', path: '/settings', adminOnly: true },
];

function Nav() {
  const { roles } = useUser();
  const isAdmin = roles.includes('admin');
  const [path, setPath] = useState(getHashPath());

  useEffect(() => {
    const handler = () => setPath(getHashPath());
    window.addEventListener('hashchange', handler);
    return () => window.removeEventListener('hashchange', handler);
  }, []);

  return (
    <List>
      <Section header="Navigation">
        {navItems.filter((item) => (item.adminOnly ? isAdmin : true)).map((item) => {
          const isActive = path === item.path;
          return (
            <Cell
              key={item.path}
              onClick={() => navigate(item.path)}
              titleBadge={isActive ? <Badge type="dot" mode="primary" /> : undefined}
              subtitle={item.adminOnly ? 'Admin only' : undefined}
              after={<Navigation>Open</Navigation>}
            >
              {item.label}
            </Cell>
          );
        })}
      </Section>
    </List>
  );
}

function RouteRenderer() {
  const route = matchRoute(getHashPath());
  const { roles } = useUser();
  const isAdmin = roles.includes('admin');

  switch (route.name) {
    case 'dashboard':
      return <Dashboard />;
    case 'inbox':
      return <Inbox />;
    case 'calls':
      return <Calls />;
    case 'callConsole':
      return <CallConsole callSid={route.params.callSid} />;
    case 'scripts':
      return <Scripts />;
    case 'users':
      return isAdmin ? <Users /> : <div className="panel">Admin access required.</div>;
    case 'settings':
      return isAdmin ? <Settings /> : <div className="panel">Admin access required.</div>;
    default:
      return <div className="panel">Route not found.</div>;
  }
}

export function AppShell() {
  const { status, user, roles, error, refresh } = useUser();
  const isAdmin = roles.includes('admin');
  const [isOnline, setIsOnline] = useState(() => (typeof navigator !== 'undefined' ? navigator.onLine : true));

  useEffect(() => {
    const handleBack = () => {
      if (window.history.length > 1) {
        window.history.back();
      } else {
        navigate('/');
      }
    };
    backButton.show();
    const off = backButton.onClick(handleBack);
    return () => {
      off();
      backButton.hide();
    };
  }, []);

  useEffect(() => {
    const handleOnline = () => setIsOnline(true);
    const handleOffline = () => setIsOnline(false);
    window.addEventListener('online', handleOnline);
    window.addEventListener('offline', handleOffline);
    return () => {
      window.removeEventListener('online', handleOnline);
      window.removeEventListener('offline', handleOffline);
    };
  }, []);

  const headerSubtitle = useMemo(() => {
    if (status === 'loading') return 'Authorizing...';
    if (status === 'error') return error || 'Auth failed';
    if (!user) return 'Not connected';
    const roleLabel = isAdmin ? 'Admin' : 'Viewer';
    return `${roleLabel} - ${user.username || user.first_name || user.id}`;
  }, [status, error, user, isAdmin]);

  return (
    <CallsProvider>
      <div className="app-shell">
        {!isOnline && (
          <Banner
            type="inline"
            header="You're offline"
            description="Some data may be outdated. Reconnect to refresh."
          />
        )}
        <Banner
          type="section"
          header="VOICEDNUT"
          subheader="mini app"
          description={headerSubtitle}
        >
          <Button size="s" mode="bezeled" onClick={refresh}>
            Refresh
          </Button>
        </Banner>
        <Nav />
        <main className="content">
          <RouteRenderer />
        </main>
      </div>
    </CallsProvider>
  );
}
