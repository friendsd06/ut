# Create type-safe conditions
conditions = [
    when(
        coalesce(col(f"source.{c}").cast("string"), "null") != coalesce(col(f"target.{c}").cast("string"), "null"),
        "Mismatch"
    )
        .when(
        col(f"source.{c}").isNull() & col(f"target.{c}").isNotNull(),
        "Missing in Source"
    )
        .when(
        col(f"source.{c}").isNotNull() & col(f"target.{c}").isNull(),
        "Missing in Target"
    )
        .otherwise("Match")
    for c in columns_to_check
]