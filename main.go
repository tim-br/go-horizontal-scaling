package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "math/rand"
    "net/http"
    "strconv"
    "time"
    "github.com/google/uuid"
    clientv3 "go.etcd.io/etcd/client/v3"
)

const (
    etcdEndpoint = "localhost:2379"
    serviceName  = "multiply-service"
)

type ServiceInstance struct {
    ID       string            `json:"id"`
    Address  string            `json:"address"`
    Port     int              `json:"port"`
    MetaData map[string]string `json:"metadata"`
}

func connectEtcd() (*clientv3.Client, error) {
    client, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{etcdEndpoint},
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        return nil, fmt.Errorf("failed to connect to etcd: %v", err)
    }
    return client, nil
}

func registerService(client *clientv3.Client, instance ServiceInstance) error {
    instanceData, err := json.Marshal(instance)
    if err != nil {
        return fmt.Errorf("failed to marshal instance data: %v", err)
    }

    lease, err := client.Grant(context.Background(), 10)
    if err != nil {
        return fmt.Errorf("failed to create lease: %v", err)
    }

    key := fmt.Sprintf("/services/%s/%s", serviceName, instance.ID)
    
    _, err = client.Put(context.Background(), key, string(instanceData), clientv3.WithLease(lease.ID))
    if err != nil {
        return fmt.Errorf("failed to register service: %v", err)
    }

    keepAliveCh, err := client.KeepAlive(context.Background(), lease.ID)
    if err != nil {
        return fmt.Errorf("failed to keep lease alive: %v", err)
    }

    go func() {
        for {
            select {
            case resp := <-keepAliveCh:
                if resp == nil {
                    log.Printf("Lost lease keep-alive for instance %s", instance.ID)
                    return
                }
            }
        }
    }()

    return nil
}

// Simulate a service that can only handle N concurrent resource-intensive tasks
var maxConcurrent = make(chan struct{}, 1) // Only 1 concurrent request

func main() {
    etcdClient, err := connectEtcd()
    if err != nil {
        log.Fatal(err)
    }
    defer etcdClient.Close()

    instance := ServiceInstance{
        ID:      uuid.New().String(),
        Address: "localhost",
        Port:    5001,
        MetaData: map[string]string{
            "version": "1.0",
        },
    }

    if err := registerService(etcdClient, instance); err != nil {
        log.Fatal(err)
    }

    http.HandleFunc("/multiply/", func(w http.ResponseWriter, r *http.Request) {
        num, _ := strconv.Atoi(r.URL.Path[len("/multiply/"):])

        // select is Go's way of handling multiple channel operations
        // It will execute the first case that's ready to proceed
        select {
            // Try to send to maxConcurrent channel (acquire semaphore)
            case maxConcurrent <- struct{}{}:
                // If we get here, we successfully acquired the semaphore
                // This only happens if the channel isn't full (we haven't hit our concurrent limit)
                log.Printf("✅ Resource slot acquired - processing...")
    
                // defer will run when this function returns
                // This ensures we always release the semaphore, even if processing panics
                defer func() { 
                    // Receive from channel to release the semaphore
                    <-maxConcurrent 
                    log.Printf("♻️  Resource slot freed")
                }()
    
            // default case runs immediately if the maxConcurrent channel is full
            // This means we've hit our concurrent processing limit
            default:
                // We couldn't acquire the semaphore, so we're at capacity
                // Instead of waiting, we immediately reject the request
                log.Printf("❌ No resource slots available - service at capacity!")
                http.Error(w, "Service at capacity", http.StatusServiceUnavailable)
                return
        }
    
        // If we get here, we have successfully acquired the semaphore
        // and can proceed with our resource-intensive work...


        time.Sleep(time.Duration(6+rand.Float64()*8) * time.Second)
        log.Printf("returning the result for %d", num)
        
        json.NewEncoder(w).Encode(map[string]int{
            "result": num * 2,
        })
    })

    log.Printf("Service instance %s starting on port %d", instance.ID, instance.Port)
    log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", instance.Port), nil))
}
