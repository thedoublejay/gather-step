export function summarizeSearchResults(results: Array<{ count: number }>) {
  let matchingCount = 0;
  let staleCount = 0;

  for (const result of results) {
    if (result.count > 0) {
      matchingCount += result.count;
    } else {
      staleCount += 1;
    }
  }

  return {
    matchingCount,
    staleCount,
    totalCount: matchingCount + staleCount,
  };
}
