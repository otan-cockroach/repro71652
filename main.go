package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	pgurl        = flag.String("url", "postgresql://root@127.0.0.1:26257/defaultdb?sslmode=disable", "pg_url")
	iterations   = flag.Int("iterations", 100, "number of iterations to run")
	writeThreads = flag.Int("write_threads", 3, "number of write threads to run")
	readThreads  = flag.Int("read_threads", 10, "number of write threads to run")
)

func main() {
	flag.Parse()
	ctx := context.Background()

	pool, err := pgxpool.Connect(ctx, *pgurl)
	if err != nil {
		log.Fatal(err)
	}

	func() {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			os.Exit(1)
		}
		defer conn.Release()

		if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS developers"); err != nil {
			log.Fatalf("failed to drop db: %v", err)
		}

		if _, err := conn.Exec(ctx, `
	CREATE TABLE "developers" ("id" bigserial primary key, "name" character varying, "first_name" character varying, "salary" int DEFAULT '70000', "firm_id" bigint, "mentor_id" int, "legacy_created_at" timestamp, "legacy_updated_at" timestamp, "legacy_created_on" timestamp, "legacy_updated_on" timestamp);
	`); err != nil {
			log.Fatalf("failed to drop db: %v", err)
		}

		if _, err := conn.Exec(ctx, `insert into developers (id, salary) values (1, 80000);`); err != nil {
			log.Fatalf("failed to drop db: %v", err)
		}
	}()

	for i := 0; i < *iterations; i++ {
		fmt.Printf("iteration %d\n", i)

		var wg sync.WaitGroup
		for t := 0; t < *writeThreads; t++ {
			// Run the following:
			// * Select the row
			// * Update salary to 200000
			// * Select the row, check it is = 200000
			// * Update salary to 80000
			// * Commit
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := pool.Acquire(ctx)
				if err != nil {
					os.Exit(1)
				}
				defer conn.Release()
				func() {
					tx, err := conn.Begin(ctx)
					defer func() { _ = tx.Rollback(ctx) }()
					if err != nil {
						log.Fatalf("failed to start txn: %v", err)
					}

					for _, cmd := range []string{
						`SELECT "developers"."id", "developers"."name", "developers"."salary", "developers"."firm_id", "developers"."mentor_id", "developers"."legacy_created_at", "developers"."legacy_updated_at", "developers"."legacy_created_on", "developers"."legacy_updated_on" FROM "developers" WHERE "developers"."id" = 1 LIMIT 1;`,
						`UPDATE "developers" SET "salary" = 200000 WHERE "developers"."id" = 1;`,
					} {
						if _, err := tx.Exec(ctx, cmd); err != nil {
							fmt.Printf("failed to run cmd %q: %#v\n", cmd, err)
							return
						}
					}

					var s int
					if err := tx.QueryRow(ctx, `SELECT "developers"."salary" FROM "developers" WHERE "developers"."id" = 1 LIMIT 1`).Scan(&s); err != nil {
						log.Fatalf("failed to get salary: %v", err)
					}

					if s != 200000 {
						fmt.Printf("read salary, expect temporary value, got %s\n", s)
						panic("f")
					}

					for _, cmd := range []string{
						fmt.Sprintf(`UPDATE "developers" SET "salary" = 80000 WHERE "developers"."id" = 1;`),
						`SELECT "developers"."id", "developers"."name", "developers"."salary", "developers"."firm_id", "developers"."mentor_id", "developers"."legacy_created_at", "developers"."legacy_updated_at", "developers"."legacy_created_on", "developers"."legacy_updated_on" FROM "developers" WHERE "developers"."id" = 1 LIMIT 1;`,
					} {
						if _, err := tx.Exec(ctx, cmd); err != nil {
							fmt.Printf("failed to run cmd %q: %#v\n", cmd, err)
							return
						}
					}

					if err := tx.Commit(ctx); err != nil {
						fmt.Printf("failed to commit: %v", err)
						return
					}
				}()
			}()
		}

		for t := 0; t < *readThreads; t++ {
			// Always check we read 80000, as we never commit 200000
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := pool.Acquire(ctx)
				if err != nil {
					os.Exit(1)
				}
				defer conn.Release()
				time.Sleep(5 * time.Millisecond)
				func() {
					tx, err := conn.Begin(ctx)
					if err != nil {
						log.Fatalf("failed to start txn: %v", err)
					}
					defer func() { _ = tx.Rollback(ctx) }()

					var s int
					if err := tx.QueryRow(ctx, `SELECT "developers"."salary" FROM "developers" WHERE "developers"."id" = 1 LIMIT 1`).Scan(&s); err != nil {
						log.Fatalf("failed to get salary: %v", err)
					}

					if s != 80000 {
						fmt.Printf("reproduced, got %d\n", s)
						panic("f")
					}

					if err := tx.Commit(ctx); err != nil {
						log.Fatalf("failed to commit: %v", err)
					}
				}()
			}()
		}
		wg.Wait()
	}
	fmt.Printf("done!\n")
}
