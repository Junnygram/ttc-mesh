import time
import requests

def test_pipeline():
    print("========================================")
    print("Testing TTC-Mesh System Design Pipeline")
    print("========================================")
    api_url = "http://localhost:8000"
    
    # 1. Health check (Block 10: HTTP)
    print("\n[1] Checking REST API Root (HTTP 200)...")
    r = requests.get(f"{api_url}/api/transit/stats", timeout=5)
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    stats = r.json()
    print(f"    [OK] Stats received: {stats}")

    # 2. GraphQL Check (Block 16: GraphQL)
    print("\n[2] Checking GraphQL Endpoint (/graphql)...")
    graphql_query = """
    query {
      stats {
        activeVehicles
        status
      }
      vehicles(limit: 2) {
        id
        route
        speed
      }
    }
    """
    gr = requests.post(f"{api_url}/graphql", json={"query": graphql_query}, timeout=5)
    assert gr.status_code == 200, f"GraphQL query failed: {gr.text}"
    gdata = gr.json()
    print(f"    [OK] GraphQL response (Zero over-fetching): {gdata['data']}")

    # 3. Webhook Registration Check (Block 20: Webhooks)
    print("\n[3] Testing Webhook Registration API...")
    wh_res = requests.post(
        f"{api_url}/api/webhooks/subscribe",
        json={"url": "https://example.com/transit-alerts", "event_types": ["TRAFFIC_CONGESTION_ALERT"]},
        timeout=5
    )
    assert wh_res.status_code == 200
    print(f"    [OK] Webhook registered: {wh_res.json()}")

    # 4. Prometheus Metrics Check (Observability)
    print("\n[4] Verifying Prometheus Telemetry Endpoint (/metrics)...")
    pm_res = requests.get(f"{api_url}/metrics", timeout=5)
    assert pm_res.status_code == 200
    assert "http_requests_total" in pm_res.text
    print("    [OK] Prometheus metrics exposed successfully.")

    print("\n🎉 ALL SYSTEM DESIGN BUILDING BLOCKS VERIFIED GREEN!")

if __name__ == "__main__":
    test_pipeline()
