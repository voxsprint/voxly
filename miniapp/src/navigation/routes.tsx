import type { ComponentType, JSX } from 'react';

import { Dashboard } from '@/routes/Dashboard';
import { Inbox } from '@/routes/Inbox';
import { Calls } from '@/routes/Calls';
import { CallConsole } from '@/routes/CallConsole';
import { Settings } from '@/routes/Settings';
import { Scripts } from '@/routes/Scripts';
import { Users } from '@/routes/Users';

export interface Route {
  path: string;
  name: string;
  Component: ComponentType;
  title?: string;
  icon?: JSX.Element;
}

export const routes: Route[] = [
  { path: '/', name: 'dashboard', Component: Dashboard, title: 'Dashboard' },
  { path: '/inbox', name: 'inbox', Component: Inbox, title: 'Inbox' },
  { path: '/calls', name: 'calls', Component: Calls, title: 'Calls' },
  { path: '/calls/:callSid', name: 'callConsole', Component: CallConsole, title: 'Call Console' },
  { path: '/settings', name: 'settings', Component: Settings, title: 'Settings' },
  { path: '/scripts', name: 'scripts', Component: Scripts, title: 'Scripts' },
  { path: '/users', name: 'users', Component: Users, title: 'Users' },
];
