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
    legacySeatIds: account.seats.map((seat) => seat.id),
  };
}

export async function readLegacyAccountProjection(collection: any) {
  return collection
    .find({ legacySeatIds: { $exists: true } })
    .project({ legacySeatIds: 1, accountStatus: 1 })
    .toArray();
}
