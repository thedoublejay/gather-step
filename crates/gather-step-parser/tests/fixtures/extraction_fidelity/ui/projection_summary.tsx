type Props = {
  rows: Array<{ amount: number; id: string }>;
};

export function ProjectionSummary(props: Props) {
  const viewModel = {
    invoiceItemTotal: props.rows.reduce((sum, row) => sum + row.amount, 0),
    rowIds: props.rows.map((row) => row.id),
  };
  return <span>{viewModel.invoiceItemTotal}</span>;
}
