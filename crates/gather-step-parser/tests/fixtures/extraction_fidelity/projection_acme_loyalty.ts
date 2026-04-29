type AcmeMember = {
  pointEvents: Array<{ value: number }>;
  households: Array<{ id: string }>;
};

export function toAcmeLoyaltyProjection(member: AcmeMember) {
  return {
    loyaltyPointTotal: member.pointEvents.reduce((sum, event) => sum + event.value, 0),
    householdIds: member.households.map((household) => household.id),
  };
}
