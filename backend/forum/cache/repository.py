import logging

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func, select

from forum.auth.schemas import UserRead
from forum.post.models import Post
from forum.thread.models import Thread

log = logging.getLogger(__name__)

RECENT_USERS_KEY = "recent_users"
LAST_N = 10
NUM_POSTS_PER_USER = "user_posts"
NUM_POSTS_PER_FORUM = "forum_posts"
NUM_THREADS_PER_FORUM = "forum_threads"


class CacheRepository:
    """
    Repository for caching. Some of the features are:
    - Caching user activity,
    - Caching thread/post counts
    - Loading from database
    """

    async def push_recent_user(self, cache: Redis, user: UserRead):
        """Push user to cache to keep track of the recent users."""
        user_data = UserRead.model_validate(user)
        try:
            async with cache.pipeline(transaction=True) as pipe:
                await pipe.lpush(RECENT_USERS_KEY, user_data.model_dump_json())  # type: ignore
                await pipe.ltrim(RECENT_USERS_KEY, 0, LAST_N - 1)  # type: ignore
                await pipe.execute()
            log.info(f"Cached {user}")
        except Exception as e:
            log.error(f"Error during caching new user: {user}: {e}")

    async def get_recent_users(self, cache: Redis) -> list[UserRead]:
        """Retrieve the recent registered users (up to LAST_n)."""
        users = await cache.lrange(RECENT_USERS_KEY, 0, LAST_N - 1)  # type: ignore
        return [UserRead.model_validate_json(user) for user in users]

    async def on_post_created(self, cache: Redis, user_id: int, forum_id: int):
        """
        Update cache after a post is created.
        Increments user total posts and forum total posts.
        """
        async with cache.pipeline() as pipe:
            await pipe.incr(f"{NUM_POSTS_PER_USER}:{user_id}")
            await pipe.incr(f"{NUM_POSTS_PER_FORUM}:{forum_id}")
            await pipe.execute()

    async def on_post_deleted(self, cache: Redis, user_id: int, forum_id: int):
        """
        Update cache after a post is deleted.
        Decrements user total posts and forum total posts.
        """
        async with cache.pipeline() as pipe:
            await pipe.decr(f"{NUM_POSTS_PER_USER}:{user_id}")
            await pipe.decr(f"{NUM_POSTS_PER_FORUM}:{forum_id}")
            await pipe.execute()

    async def on_thread_created(self, cache: Redis, forum_id: int):
        """
        Update cache after a thread is created.
        Increments forum total threads.
        """
        await cache.incr(f"{NUM_THREADS_PER_FORUM}:{forum_id}")

    async def on_thread_deleted(self, cache: Redis, forum_id: int):
        """
        Update cache after a thread is deleted.
        Decrements forum total threads.
        """
        await cache.decr(f"{NUM_THREADS_PER_FORUM}:{forum_id}")

    async def on_forum_read(self, cache: Redis, forum_id: int) -> tuple[int, int]:
        """
        Read cache for a single forum.
        Returns the total posts and the number of threads.
        """
        async with cache.pipeline(transaction=False) as pipe:
            await pipe.get(f"{NUM_POSTS_PER_FORUM}:{forum_id}")
            await pipe.get(f"{NUM_THREADS_PER_FORUM}:{forum_id}")
            res = await pipe.execute()
        return (res[0] or 0, res[1] or 0)

    async def get_user_total_posts(self, cache: Redis, user_id: int) -> int | None:
        """Get the total number of posts of a user."""
        return await cache.get(f"{NUM_POSTS_PER_USER}:{user_id}")

    async def load_from_db(self, cache: Redis, db_session: AsyncSession):
        """Load cache with data from the database."""
        await self._load_threads(cache, db_session)
        await self._load_posts(cache, db_session)
        await self._load_user_posts(cache, db_session)

    async def _load_threads(self, cache: Redis, db_session: AsyncSession):
        """Load thread count per forum from database to cache."""
        stmt = select(Thread.forum_id, func.count().label("n_threads")).group_by(
            Thread.forum_id
        )
        res = await db_session.execute(stmt)

        async with cache.pipeline() as pipe:
            for row in res.all():
                await pipe.set(f"{NUM_THREADS_PER_FORUM}:{row.forum_id}", row.n_threads)
            await pipe.execute()

    async def _load_posts(self, cache: Redis, db_session: AsyncSession):
        """Load post count per forum from database to cache."""
        stmt = (
            select(Thread.forum_id, func.count().label("n_posts"))
            .select_from(Post)
            .join(Thread)
            .group_by(Thread.forum_id)
        )

        res = await db_session.execute(stmt)

        async with cache.pipeline() as pipe:
            for row in res.all():
                await pipe.set(f"{NUM_POSTS_PER_FORUM}:{row.forum_id}", row.n_posts)
            await pipe.execute()

    async def _load_user_posts(self, cache: Redis, db_session: AsyncSession):
        """Load post count per user from database to cache."""
        stmt = select(Post.author_id, func.count().label("n_posts")).group_by(
            Post.author_id
        )

        res = await db_session.execute(stmt)

        async with cache.pipeline() as pipe:
            for row in res.all():
                await pipe.set(f"{NUM_POSTS_PER_USER}:{row.author_id}", row.n_posts)
            await pipe.execute()


cache_repo = CacheRepository()
