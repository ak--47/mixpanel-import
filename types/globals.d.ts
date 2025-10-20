// Global type definitions for browser environment

// Mixpanel SDK
interface Mixpanel {
  init(token: string, config?: any): void;
  track(event_name: string, properties?: any): void;
  identify(user_id: string): void;
  register(properties: any): void;
  opt_in_tracking(): void;
  opt_out_tracking(): void;
  start_session_recording(): void;
  people: {
    set(properties: any): void;
    set_once(properties: any): void;
    increment(property: string, value?: number): void;
    append(property: string, value: any): void;
    union(property: string, value: any[]): void;
    track_charge(amount: number, properties?: any): void;
    clear_charges(): void;
    delete_user(): void;
  };
}

// Declare global mixpanel
declare const mixpanel: Mixpanel;

// Extend Window interface to include mixpanel
interface Window {
  mixpanel: Mixpanel;
}

// Dropzone (if used in import.js)
declare const Dropzone: any;

// Monaco Editor (if used)
declare const monaco: any;
