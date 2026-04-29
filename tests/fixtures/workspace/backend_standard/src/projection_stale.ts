type Seat = {
  id: string;
};

type LegacyAccountDocument = {
  seats: Seat[];
  status: 'trial' | 'paid';
};

export function buildLegacyAccountProjection(account: LegacyAccountDocument) {
  return {
    accountStatus: account.status,
    legacySeatIds: account.seats,
  };
}

export const legacyAccountProjectionFilterBySeat = { legacySeatIds: { $exists: true } };

export const legacyAccountProjectionReadModel = { legacySeatIds: 1, accountStatus: 1 };
