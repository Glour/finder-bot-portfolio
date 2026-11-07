#!/usr/bin/env python3
import asyncio
import sys
# Set PYTHONPATH to the backend directory when running this script.
from infrastructure.database.setup import db_manager
from settings import settings
from sqlalchemy import text

async def main():
    db_manager.init_database(settings)
    session_maker = db_manager.async_session
    
    async with session_maker() as session:
        # Fix base_asset for FUTURES
        result = await session.execute(text('''
            UPDATE tokens
            SET base_asset = base_asset || '_FUTURES'
            WHERE market_type = 'FUTURES'
              AND base_asset NOT LIKE '%_FUTURES'
              AND base_asset NOT LIKE '%FUTURES'
        '''))
        print(f"Updated {result.rowcount} FUTURES tokens")
        await session.commit()
    
    print("✅ Cleanup completed!")

asyncio.run(main())
