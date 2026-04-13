export function process_payment(amount: number, currency: string): boolean {
  return amount > 0 && currency.length > 0;
}
