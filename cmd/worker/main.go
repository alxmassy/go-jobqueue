package main 

import (
	"context"
	"log"
	"time"
	"jobqueue/internal/store"
)

func main() {
	connStr := "postgres://alx:strong@localhost/jobqueue?sslmode=disable"

	s, err := store.NewStore(connStr)
	if err != nil {
		log.Fatalf("failed to connect to store: %v", err)
	}

	go func ()  {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			n, err := s.ReapExpiredJobs(context.Background())
			if err != nil {
				log.Printf("failed to recover leased jobs: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("recovered %d leased jobs\n", n)
			}
		}
	} ()

	log.Println("Worker started, waiting for jobs...")
	
	for {
		job, err := s.ClaimJob(context.Background(), 30*time.Second)
		if err != nil {
			log.Printf("failed to claim job: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if job == nil {
			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("Claimed job %s type %s attempts %d/%d\n", job.ID, job.Type, job.Attempts, job.MaxAttempts)

		time.Sleep(2 * time.Second)

		if job.Attempts == 1 {
			errMsg := "simulated job failure"

			if job.Attempts < job.MaxAttempts {
				backoff := time.Duration(job.Attempts*5) * time.Second

				log.Printf("Job %s failed, will retry after %v: %s\n", job.ID, backoff, errMsg)

				if err := s.MarkRetry(context.Background(), job.ID, errMsg, backoff); err != nil {
					log.Printf("failed to retry job %s: %v", job.ID, err)
				}

				continue
			}
		
			if err := s.MarkFailed(context.Background(), job.ID, errMsg); err != nil {
				log.Printf("failed to mark job %s as failed: %v", job.ID, err)
			}

			continue
		}

		if err := s.MarkSucceeded(context.Background(), job.ID); err != nil {
			log.Printf("failed to mark job %s as succeeded: %v", job.ID, err)
			continue
		}

		log.Printf("Completed job %s\n", job.ID)
	}
}