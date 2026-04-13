import * as X from /* c */ "./x";
export { Y } from "./y"; // tail comment

export function useImports() {
  return [X, Y];
}
