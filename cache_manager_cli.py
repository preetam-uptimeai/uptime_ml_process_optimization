#!/usr/bin/env python3
"""
Command-line interface for managing the MinIO cache system.
"""

import argparse
import sys
import os
from datetime import datetime

# Add the project root to Python path
sys.path.append(os.path.dirname(__file__))

from utils import get_cache_manager, get_minio_client


def show_cache_stats(cache_manager):
    """Display comprehensive cache statistics."""
    print("📊 Cache Statistics")
    print("=" * 60)
    
    stats = cache_manager.get_cache_stats()
    
    # Show current config version first
    current_version = stats.get('current_config_version')
    if current_version:
        print(f"🔧 Current Config Version: {current_version}")
    else:
        print(f"🔧 Current Config Version: Not set")
    
    # Show cached timestamp
    cached_timestamp = stats.get('cached_last_run_timestamp')
    if cached_timestamp:
        print(f"🕒 Cached Last Run Timestamp: {cached_timestamp}")
    else:
        print(f"🕒 Cached Last Run Timestamp: Not set")
    print()
    
    total_active = 0
    total_expired = 0
    
    for cache_type, cache_stats in stats.items():
        if cache_type in ['current_config_version', 'cached_last_run_timestamp']:
            continue  # Skip version and timestamp info, already shown
            
        active = cache_stats['active_items']
        expired = cache_stats['expired_items']
        total_active += active
        total_expired += expired
        
        print(f"📦 {cache_type.replace('_', ' ').title()}:")
        print(f"   ✅ Active items: {active}")
        print(f"   ⏳ Expired items: {expired}")
        print(f"   📋 Total items: {cache_stats['total_items']}")
        
        if cache_stats['cache_keys']:
            print(f"   🔑 Keys: {', '.join(cache_stats['cache_keys'][:3])}")
            if len(cache_stats['cache_keys']) > 3:
                print(f"        ... and {len(cache_stats['cache_keys']) - 3} more")
        print()
    
    print(f"📈 Summary:")
    print(f"   Total active items: {total_active}")
    print(f"   Total expired items: {total_expired}")
    print(f"   Overall cache efficiency: {total_active/(total_active+total_expired)*100:.1f}%" if (total_active+total_expired) > 0 else "   No items in cache")


def clear_all_caches(cache_manager):
    """Clear all caches."""
    print("🗑️ Clearing All Caches")
    print("=" * 60)
    
    # Show stats before clearing
    stats_before = cache_manager.get_cache_stats()
    total_items_before = sum(
        cache_stats['total_items'] for cache_stats in stats_before.values() 
        if isinstance(cache_stats, dict) and 'total_items' in cache_stats
    )
    
    # Clear caches
    cache_manager.clear_all_caches()
    
    # Show stats after clearing
    stats_after = cache_manager.get_cache_stats()
    total_items_after = sum(
        cache_stats['total_items'] for cache_stats in stats_after.values() 
        if isinstance(cache_stats, dict) and 'total_items' in cache_stats
    )
    
    print(f"✅ Cleared {total_items_before - total_items_after} items from cache")
    print("🎯 All caches are now empty")


def cleanup_expired_files(cache_manager):
    """Clean up expired temporary files."""
    print("🧹 Cleaning Up Expired Temporary Files")
    print("=" * 60)
    
    # Show cache stats before cleanup
    stats_before = cache_manager.get_cache_stats()
    temp_files_before = stats_before.get('temp_files_cache', {}).get('total_items', 0)
    
    # Perform cleanup
    cache_manager.cleanup_expired_temp_files()
    
    # Show cache stats after cleanup
    stats_after = cache_manager.get_cache_stats()
    temp_files_after = stats_after.get('temp_files_cache', {}).get('total_items', 0)
    
    cleaned_files = temp_files_before - temp_files_after
    print(f"✅ Cleaned up {cleaned_files} expired temporary files")
    print(f"📁 Remaining temporary files: {temp_files_after}")


def test_cache_functionality(cache_manager):
    """Test basic cache functionality."""
    print("🧪 Testing Cache Functionality")
    print("=" * 60)
    
    try:
        from utils import ConfigManager
        
        config_manager = ConfigManager()
        
        # Test config loading
        print("1️⃣ Testing config loading...")
        start_stats = cache_manager.get_cache_stats()
        
        config = config_manager.load_strategy_config_from_minio()
        
        end_stats = cache_manager.get_cache_stats()
        
        # Check if cache was populated
        config_items_before = start_stats.get('config_cache', {}).get('active_items', 0)
        config_items_after = end_stats.get('config_cache', {}).get('active_items', 0)
        
        if config_items_after > config_items_before:
            print("   ✅ Config successfully cached")
        else:
            print("   ✅ Config loaded (may have been already cached)")
        
        # Test second load (should be faster)
        print("2️⃣ Testing cached config loading...")
        config2 = config_manager.load_strategy_config_from_minio()
        
        if config == config2:
            print("   ✅ Cached config is consistent")
        else:
            print("   ⚠️ Cached config differs from original")
        
        print("🎉 Cache functionality test completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Cache functionality test failed: {e}")
        return False


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Cache Manager CLI for MinIO Process Optimization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cache_manager_cli.py --stats          # Show cache statistics
  python cache_manager_cli.py --clear          # Clear all caches
  python cache_manager_cli.py --cleanup        # Clean up expired files
  python cache_manager_cli.py --test           # Test cache functionality
  python cache_manager_cli.py --all            # Run all operations
        """
    )
    
    parser.add_argument('--stats', action='store_true', 
                       help='Show cache statistics')
    parser.add_argument('--clear', action='store_true',
                       help='Clear all caches')
    parser.add_argument('--cleanup', action='store_true',
                       help='Clean up expired temporary files')
    parser.add_argument('--test', action='store_true',
                       help='Test cache functionality')
    parser.add_argument('--all', action='store_true',
                       help='Run all operations (stats, test, cleanup)')
    
    args = parser.parse_args()
    
    # If no arguments provided, show help
    if not any(vars(args).values()):
        parser.print_help()
        return
    
    print("🎛️ MinIO Cache Manager CLI")
    print("=" * 60)
    print(f"📅 Started at: {datetime.now()}")
    print()
    
    # Get cache manager instance
    cache_manager = get_cache_manager()
    
    # Execute requested operations
    if args.all or args.stats:
        show_cache_stats(cache_manager)
        print()
    
    if args.all or args.test:
        test_cache_functionality(cache_manager)
        print()
    
    if args.all or args.cleanup:
        cleanup_expired_files(cache_manager)
        print()
    
    if args.clear:
        clear_all_caches(cache_manager)
        print()
    
    # Show final stats if we did operations
    if args.all or args.clear or args.cleanup:
        print("📊 Final Cache Statistics:")
        print("-" * 30)
        show_cache_stats(cache_manager)
    
    print(f"✅ Operations completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
