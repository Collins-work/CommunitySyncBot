from __future__ import annotations

from config import Settings
from db import PostRepository


def fmt_bool(value: object) -> str:
    return "yes" if bool(value) else "no"


def main() -> None:
    settings = Settings.from_env()
    repo = PostRepository(settings.database_url)
    repo.open()

    try:
        snapshot = repo.get_dashboard_snapshot(recent_limit=20)
    finally:
        repo.close()

    stats = snapshot.get("stats", {})
    schedules = snapshot.get("schedules", {})
    recent = snapshot.get("recent", [])

    print("=== CommunitySyncBot Dashboard ===")
    print(f"Total posts: {stats.get('total_posts', 0)}")
    print(f"Unposted: {stats.get('unposted_posts', 0)}")
    print(f"Posted: {stats.get('posted_posts', 0)}")
    print(f"In progress: {stats.get('in_progress_posts', 0)}")
    print(f"Targets: {stats.get('target_count', 0)}")
    print(f"Active schedules: {schedules.get('active_schedules', 0)}")
    print(f"Total schedules: {schedules.get('total_schedules', 0)}")
    print("")
    print("Latest posts:")

    if not recent:
        print("  (no posts found)")
        return

    for row in recent:
        preview = (row.get("preview") or "").replace("\n", " ").strip()
        if len(preview) > 80:
            preview = preview[:77] + "..."
        print(
            "  "
            f"id={row.get('id')} "
            f"target={row.get('target_chat_id')} "
            f"type={row.get('content_type')} "
            f"posted={fmt_bool(row.get('posted'))} "
            f"in_progress={fmt_bool(row.get('in_progress'))} "
            f"created_at={row.get('created_at')} "
            f"posted_at={row.get('posted_at')} "
            f"preview=\"{preview}\""
        )


if __name__ == "__main__":
    main()
