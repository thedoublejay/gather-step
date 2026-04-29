export function logProjectionDebug(logger: { info(message: string): void }) {
  logger.info('invoiceItemTotal and customerIds were present in the payload');
  const message = 'projection mapping invoiceItemTotal customerIds';
  return message;
}
