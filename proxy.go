package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "time"
    clientv3 "go.etcd.io/etcd/client/v3"
)

type ServiceInstance struct {
    ID       string            `json:"id"`
    Address  string            `json:"address"`
    Port     int              `json:"port"`
    MetaData map[string]string `json:"metadata"`
}

func main() {
    // Connect to etcd
    client, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{"localhost:2379"},
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Create a proxy handler
    http.HandleFunc("/multiply/", func(w http.ResponseWriter, r *http.Request) {
        log.Println("Proxy received request, fetching available services...")
        
        // Get all services
        resp, err := client.Get(context.Background(), "/services/multiply-service/", clientv3.WithPrefix())
        if err != nil {
            http.Error(w, "Failed to get services", http.StatusInternalServerError)
            return
        }

        if len(resp.Kvs) == 0 {
            http.Error(w, "No services available", http.StatusServiceUnavailable)
            return
        }

        // For now, just use the first available service
        var instance ServiceInstance
        if err := json.Unmarshal(resp.Kvs[0].Value, &instance); err != nil {
            http.Error(w, "Failed to parse service data", http.StatusInternalServerError)
            return
        }

        // Forward the request
        targetURL := fmt.Sprintf("http://%s:%d%s", instance.Address, instance.Port, r.URL.Path)
        log.Printf("Forwarding request to: %s", targetURL)

        resp2, err := http.Get(targetURL)
        if err != nil {
            http.Error(w, "Failed to forward request", http.StatusInternalServerError)
            return
        }
        defer resp2.Body.Close()

        // Copy the response back to the client
        w.WriteHeader(resp2.StatusCode)
        io.Copy(w, resp2.Body)
    })

    log.Printf("Starting proxy server on :4999")
    log.Fatal(http.ListenAndServe(":4999", nil))
}
