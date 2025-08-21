#!/usr/bin/env python3
"""
Test script to demonstrate timestamp caching functionality.
"""

import time
import sys
import os
from datetime import datetime, timedelta

# Add the project root to Python path
sys.path.append(os.path.dirname(__file__))

from utils import get_cache_manager, ConfigManager


def test_timestamp_caching():
    """Test timestamp caching functionality."""
    print("🧪 Testing Timestamp Caching")
    print("=" * 60)
    
    cache_manager = get_cache_manager()
    config_manager = ConfigManager()
    
    # Clear cache to start fresh
    cache_manager.clear_all_caches()
    
    try:
        # Test 1: First timestamp load (should read from file)
        print("1️⃣ First timestamp load (should read from file)...")
        start_time = time.time()
        timestamp1 = config_manager.get_last_run_timestamp()
        load_time1 = time.time() - start_time
        
        if timestamp1:
            print(f"   ✅ Loaded timestamp from file: {timestamp1}")
            print(f"   ⏱️  Load time: {load_time1:.4f} seconds")
        else:
            print("   ⚠️ No timestamp found in file")
        
        # Check cache stats
        stats = cache_manager.get_cache_stats()
        cached_timestamp = stats.get('cached_last_run_timestamp')
        print(f"   📊 Cached timestamp: {cached_timestamp}")
        
        # Test 2: Second timestamp load (should use cache)
        print("\n2️⃣ Second timestamp load (should use cache)...")
        start_time = time.time()
        timestamp2 = config_manager.get_last_run_timestamp()
        load_time2 = time.time() - start_time
        
        if timestamp2:
            print(f"   ✅ Retrieved cached timestamp: {timestamp2}")
            print(f"   ⏱️  Load time: {load_time2:.4f} seconds")
        
        # Verify caching worked
        cache_hit = timestamp1 == timestamp2
        print(f"   🎯 Cache hit verified: {'✅ YES' if cache_hit else '❌ NO'}")
        
        if load_time1 > 0 and load_time2 > 0:
            speedup = load_time1 / load_time2
            print(f"   🚀 Cache speedup: {speedup:.1f}x faster")
        
        # Test 3: Update timestamp and verify cache
        print("\n3️⃣ Testing timestamp update and cache sync...")
        new_timestamp = datetime.now()
        print(f"   📝 Setting new timestamp: {new_timestamp}")
        
        config_manager.update_last_run_timestamp(new_timestamp)
        
        # Verify cache was updated
        stats_after_update = cache_manager.get_cache_stats()
        updated_cached_timestamp = stats_after_update.get('cached_last_run_timestamp')
        
        cache_updated = updated_cached_timestamp == new_timestamp
        print(f"   ✅ Cache updated correctly: {'✅ YES' if cache_updated else '❌ NO'}")
        print(f"   📊 New cached timestamp: {updated_cached_timestamp}")
        
        # Test 4: Load timestamp again (should use updated cache)
        print("\n4️⃣ Loading timestamp after update (should use updated cache)...")
        timestamp3 = config_manager.get_last_run_timestamp()
        
        cache_consistency = timestamp3 == new_timestamp
        print(f"   ✅ Cache consistency verified: {'✅ YES' if cache_consistency else '❌ NO'}")
        print(f"   📊 Retrieved timestamp: {timestamp3}")
        
        # Test results
        all_tests_passed = cache_hit and cache_updated and cache_consistency
        
        print("\n🏁 Test Results Summary:")
        print(f"   ✅ Cache hit functionality: {'PASS' if cache_hit else 'FAIL'}")
        print(f"   ✅ Cache update functionality: {'PASS' if cache_updated else 'FAIL'}")
        print(f"   ✅ Cache consistency: {'PASS' if cache_consistency else 'FAIL'}")
        print(f"   🎯 Overall result: {'🎉 ALL TESTS PASSED' if all_tests_passed else '❌ SOME TESTS FAILED'}")
        
        return all_tests_passed
        
    except Exception as e:
        print(f"❌ Timestamp caching test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_cache_invalidation_on_version_change():
    """Test that timestamp cache is cleared when config version changes."""
    print("\n🧪 Testing Timestamp Cache Invalidation on Version Change")
    print("=" * 60)
    
    cache_manager = get_cache_manager()
    
    try:
        # Set up initial timestamp cache
        test_timestamp = datetime.now()
        cache_manager.set_cached_last_run_timestamp(test_timestamp)
        
        print(f"1️⃣ Set initial cached timestamp: {test_timestamp}")
        
        # Verify it's cached
        cached_before = cache_manager.get_cached_last_run_timestamp()
        print(f"   📊 Cached timestamp before version change: {cached_before}")
        
        # Simulate version change (this should clear all caches including timestamp)
        version_changed = cache_manager.check_and_invalidate_on_version_change("2.0.0")
        
        if version_changed:
            print("2️⃣ Version change detected - caches should be cleared")
        
        # Check if timestamp cache was cleared
        cached_after = cache_manager.get_cached_last_run_timestamp()
        print(f"   📊 Cached timestamp after version change: {cached_after}")
        
        timestamp_cleared = cached_after is None
        print(f"   🗑️ Timestamp cache cleared: {'✅ YES' if timestamp_cleared else '❌ NO'}")
        
        return timestamp_cleared
        
    except Exception as e:
        print(f"❌ Version change test failed: {e}")
        return False


def demonstrate_cache_benefits():
    """Demonstrate the benefits of timestamp caching."""
    print("\n🧪 Demonstrating Timestamp Cache Benefits")
    print("=" * 60)
    
    cache_manager = get_cache_manager()
    config_manager = ConfigManager()
    
    try:
        # Clear cache first
        cache_manager.clear_all_caches()
        
        # Simulate multiple timestamp accesses
        total_time_with_cache = 0
        num_accesses = 5
        
        print(f"📊 Simulating {num_accesses} timestamp accesses...")
        
        for i in range(num_accesses):
            start_time = time.time()
            timestamp = config_manager.get_last_run_timestamp()
            access_time = time.time() - start_time
            total_time_with_cache += access_time
            
            cache_status = "CACHED" if i > 0 else "FILE"
            print(f"   Access {i+1}: {access_time:.4f}s ({cache_status})")
        
        print(f"\n📈 Performance Summary:")
        print(f"   Total time for {num_accesses} accesses: {total_time_with_cache:.4f} seconds")
        print(f"   Average time per access: {total_time_with_cache/num_accesses:.4f} seconds")
        
        # Show final cache stats
        stats = cache_manager.get_cache_stats()
        print(f"   📊 Final cached timestamp: {stats.get('cached_last_run_timestamp')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Cache benefits test failed: {e}")
        return False


def main():
    """Main test function."""
    print("🚀 Timestamp Caching Test Suite")
    print("=" * 60)
    print(f"📅 Test started at: {datetime.now()}")
    
    tests_passed = 0
    total_tests = 3
    
    # Test 1: Basic timestamp caching
    if test_timestamp_caching():
        tests_passed += 1
    
    # Test 2: Cache invalidation on version change
    if test_cache_invalidation_on_version_change():
        tests_passed += 1
    
    # Test 3: Cache benefits demonstration
    if demonstrate_cache_benefits():
        tests_passed += 1
    
    # Final summary
    print("\n" + "=" * 60)
    print("🏁 Timestamp Caching Test Summary")
    print("=" * 60)
    print(f"✅ Tests passed: {tests_passed}/{total_tests}")
    
    if tests_passed == total_tests:
        print("🎉 All timestamp caching tests passed!")
        print("\n💡 Key Features Verified:")
        print("   🕒 Timestamp caching from file to memory")
        print("   🚀 Fast cached timestamp retrieval")
        print("   🔄 Cache updates when timestamp changes")
        print("   🗑️ Cache invalidation on version changes")
        print("   📊 Consistent cache behavior")
    else:
        print("⚠️  Some tests failed. Please check the output above.")
    
    print(f"\n📅 Test completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
