from millpond import metrics

# Initialize metrics with a test pipeline label so tests that don't mock metrics
# can call .labels() without hitting "Incorrect label names" errors.
metrics.init("test-topic-test-table")
