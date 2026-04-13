// Canonical shared guard class used to exercise the
// `impact UserAuthGuard.canActivate` oracle scenario.  `shared_contracts`
// matches `is_canonical_boundary` because its repo name contains
// `contract`, which lets the ranking assertion verify that the canonical
// declaration wins over peer classes with the same name.

export class UserAuthGuard {
  canActivate(): boolean {
    return true;
  }
}
