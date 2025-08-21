#!/usr/bin/env python3
"""
Test script for the Process Optimization API server.
"""

import requests
import json
import yaml
import sys
import os
from datetime import datetime

# Add the project root to Python path
sys.path.append(os.path.dirname(__file__))


def load_sample_config():
    """Load the sample config file."""
    try:
        with open('config.yaml', 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading config: {e}")
        return None


def create_sample_input_data():
    """Create sample input data for testing."""
    return {
        # Operative Variables (Degrees of Freedom)
        "Kiln_Feed_SFF_1_Feed_rate": 85.0,
        "Kiln_Feed_SFF_2_Feed_rate": 87.0,
        "Kiln_Coal_PV": 8.5,
        "Calciner_temperature_PV": 875.0,
        "PH_Fan_Speed_PV": 1200.0,
        "Kiln_Drive_Speed_PV": 3.2,
        "Under_grate_Average_Pressure": 4.5,
        "Cooler_Fan_3_Flow": 45000.0,
        "Cooler_Fan_4_Flow": 47000.0,
        "Cooler_Fan_5_Flow": 46000.0,
        "Cooler_Fan_6_Flow": 48000.0,
        "Cooler_Fan_7_Flow": 35000.0,
        "Cooler_Fan_8_Flow": 36000.0,
        "Cooler_Fan_9_Flow": 34000.0,
        "Cooler_Fan_A_Flow": 12000.0,
        "Cooler_Fan_2_Flow": 13000.0,
        
        # Informative Variables (Live Plant Data)
        "Kiln_Inlet_NOX": 0.35,
        "Kiln_Drive_Current": 520.0,
        "Clinker_temperature": 95.0,
        "Calciner_outlet_CO": 0.25
    }


def test_health_check(base_url: str):
    """Test the health check endpoint."""
    print("🏥 Testing health check endpoint...")
    
    try:
        response = requests.get(f"{base_url}/health", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ Health check passed")
            print(f"   📊 Cache stats: {data.get('cache_stats', {})}")
            return True
        else:
            print(f"   ❌ Health check failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ❌ Health check error: {e}")
        return False


def test_cache_stats(base_url: str):
    """Test the cache stats endpoint."""
    print("📊 Testing cache stats endpoint...")
    
    try:
        response = requests.get(f"{base_url}/cache/stats", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ Cache stats retrieved")
            print(f"   🔧 Config version: {data.get('current_config_version', 'Not set')}")
            
            cache_details = data.get('cache_details', {})
            for cache_type, stats in cache_details.items():
                active = stats.get('active_items', 0)
                total = stats.get('total_items', 0)
                print(f"   📦 {cache_type}: {active}/{total} active")
            
            return True
        else:
            print(f"   ❌ Cache stats failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ❌ Cache stats error: {e}")
        return False


def test_optimization_endpoint(base_url: str):
    """Test the optimization endpoint."""
    print("🔬 Testing optimization endpoint...")
    
    # Load config and input data
    config = load_sample_config()
    if not config:
        print("   ❌ Failed to load sample config")
        return False
    
    input_data = create_sample_input_data()
    
    # Prepare request
    request_data = {
        "input_data": input_data,
        "config": config
    }
    
    print(f"   📤 Sending request with {len(input_data)} input variables...")
    
    try:
        response = requests.post(
            f"{base_url}/optimize", 
            json=request_data,
            timeout=60  # Longer timeout for optimization
        )
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get('status') == 'success':
                print(f"   ✅ Optimization completed successfully")
                
                summary = result.get('summary', {})
                print(f"   📈 Optimized variables: {summary.get('total_optimized_variables', 0)}")
                print(f"   🎯 Predicted variables: {summary.get('total_predicted_variables', 0)}")
                print(f"   ⚖️ Constraints: {summary.get('total_constraints', 0)}")
                
                cost_value = result.get('cost_function_value')
                if cost_value is not None:
                    print(f"   💰 Cost function value: {cost_value:.4f}")
                
                # Show a few optimized variables
                optimized_vars = result.get('optimized_variables', {})
                if optimized_vars:
                    print(f"   🔧 Sample optimized variables:")
                    count = 0
                    for var_name, var_data in optimized_vars.items():
                        if count < 3:  # Show first 3
                            current = var_data.get('current_value')
                            optimized = var_data.get('optimized_value')
                            units = var_data.get('units', '')
                            print(f"      {var_name}: {current} → {optimized} {units}")
                            count += 1
                
                return True
            else:
                error = result.get('error', 'Unknown error')
                print(f"   ❌ Optimization failed: {error}")
                return False
        else:
            print(f"   ❌ Request failed: {response.status_code}")
            try:
                error_data = response.json()
                print(f"   📝 Error details: {error_data.get('error', 'No details')}")
            except:
                print(f"   📝 Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"   ❌ Optimization test error: {e}")
        return False


def test_invalid_requests(base_url: str):
    """Test invalid request handling."""
    print("🧪 Testing invalid request handling...")
    
    test_cases = [
        {
            "name": "Empty request body",
            "data": {},
            "expected_status": 400
        },
        {
            "name": "Missing input_data",
            "data": {"config": {}},
            "expected_status": 400
        },
        {
            "name": "Missing config",
            "data": {"input_data": {}},
            "expected_status": 400
        },
        {
            "name": "Invalid config structure",
            "data": {
                "input_data": create_sample_input_data(),
                "config": {"invalid": "config"}
            },
            "expected_status": 400
        }
    ]
    
    passed_tests = 0
    
    for test_case in test_cases:
        print(f"   🔍 Testing: {test_case['name']}")
        
        try:
            response = requests.post(
                f"{base_url}/optimize",
                json=test_case['data'],
                timeout=10
            )
            
            if response.status_code == test_case['expected_status']:
                print(f"      ✅ Correctly returned status {response.status_code}")
                passed_tests += 1
            else:
                print(f"      ❌ Expected {test_case['expected_status']}, got {response.status_code}")
                
        except Exception as e:
            print(f"      ❌ Test error: {e}")
    
    print(f"   📊 Invalid request tests: {passed_tests}/{len(test_cases)} passed")
    return passed_tests == len(test_cases)


def main():
    """Main test function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Process Optimization API')
    parser.add_argument('--host', default='localhost', help='API server host')
    parser.add_argument('--port', type=int, default=5000, help='API server port')
    parser.add_argument('--no-optimization', action='store_true', help='Skip optimization test')
    
    args = parser.parse_args()
    
    base_url = f"http://{args.host}:{args.port}"
    
    print("🚀 Process Optimization API Test Suite")
    print("=" * 60)
    print(f"📍 Testing API at: {base_url}")
    print(f"📅 Test started at: {datetime.now()}")
    print()
    
    tests_passed = 0
    total_tests = 4
    
    # Test 1: Health check
    if test_health_check(base_url):
        tests_passed += 1
    print()
    
    # Test 2: Cache stats
    if test_cache_stats(base_url):
        tests_passed += 1
    print()
    
    # Test 3: Optimization endpoint (optional)
    if not args.no_optimization:
        if test_optimization_endpoint(base_url):
            tests_passed += 1
        print()
    else:
        print("🔬 Skipping optimization test as requested")
        total_tests -= 1
        print()
    
    # Test 4: Invalid requests
    if test_invalid_requests(base_url):
        tests_passed += 1
    print()
    
    # Final summary
    print("=" * 60)
    print("🏁 API Test Summary")
    print("=" * 60)
    print(f"✅ Tests passed: {tests_passed}/{total_tests}")
    
    if tests_passed == total_tests:
        print("🎉 All API tests passed!")
        print("\n💡 API is working correctly:")
        print("   🏥 Health check endpoint responding")
        print("   📊 Cache stats endpoint working")
        if not args.no_optimization:
            print("   🔬 Optimization endpoint functional")
        print("   🛡️ Error handling working properly")
    else:
        print("⚠️  Some tests failed. Please check the output above.")
        print("\n🔧 Make sure the API server is running:")
        print(f"   python api_server.py --host {args.host} --port {args.port}")
    
    print(f"\n📅 Test completed at: {datetime.now()}")
    
    return tests_passed == total_tests


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
