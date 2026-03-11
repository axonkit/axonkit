package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/axonkit/axonkit/pkg/auth"
	"github.com/axonkit/axonkit/pkg/proto"
)

func main() {
	createCmd := flag.NewFlagSet("create", flag.ExitOnError)
	parseCmd := flag.NewFlagSet("parse", flag.ExitOnError)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "create":
		runCreate(createCmd, os.Args[2:])
	case "parse":
		runParse(parseCmd, os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `Usage: token <command> [flags]

Commands:
  create   Create a new access token
  parse    Parse and display a token's claims

Run "token <command> -h" for more information.`)
}

func runCreate(fs *flag.FlagSet, args []string) {
	apiKey := fs.String("api-key", "", "API key (required)")
	apiSecret := fs.String("api-secret", "", "API secret (required)")
	identity := fs.String("identity", "", "Participant identity (required)")
	name := fs.String("name", "", "Participant display name")
	room := fs.String("room", "", "Room name to join")
	ttl := fs.Duration("ttl", 6*time.Hour, "Token TTL")
	join := fs.Bool("join", true, "Grant room join permission")
	admin := fs.Bool("admin", false, "Grant room admin permission")
	publish := fs.Bool("publish", true, "Grant media publish permission")
	subscribe := fs.Bool("subscribe", true, "Grant media subscribe permission")
	publishData := fs.Bool("publish-data", true, "Grant data publish permission")
	hidden := fs.Bool("hidden", false, "Hide participant from others")
	recorder := fs.Bool("recorder", false, "Mark as recorder participant")
	metadata := fs.String("metadata", "", "Participant metadata")

	fs.Parse(args)

	if *apiKey == "" || *apiSecret == "" || *identity == "" {
		fmt.Fprintln(os.Stderr, "Error: --api-key, --api-secret, and --identity are required")
		fs.PrintDefaults()
		os.Exit(1)
	}

	at := auth.NewAccessToken(*apiKey, *apiSecret).
		SetIdentity(*identity).
		SetTTL(*ttl)

	if *name != "" {
		at.SetName(*name)
	}

	roomGrant := &proto.RoomGrant{
		Name:   *room,
		Join:   *join,
		Admin:  *admin,
		Hidden: *hidden,
	}
	at.SetRoomGrant(roomGrant)

	mediaGrant := &proto.MediaGrant{
		Recorder: *recorder,
	}
	mediaGrant.SetCanPublish(*publish)
	mediaGrant.SetCanSubscribe(*subscribe)
	mediaGrant.SetCanPublishData(*publishData)
	at.SetMediaGrant(mediaGrant)

	if *metadata != "" {
		at.SetMetadata(*metadata)
	}

	token, err := at.Sign()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating token: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(token)
}

func runParse(fs *flag.FlagSet, args []string) {
	apiKey := fs.String("api-key", "", "API key (for verification, optional)")
	apiSecret := fs.String("api-secret", "", "API secret (for verification, optional)")

	fs.Parse(args)

	tokenStr := fs.Arg(0)
	if tokenStr == "" {
		fmt.Fprintln(os.Stderr, "Error: token argument is required")
		fmt.Fprintln(os.Stderr, "Usage: token parse [flags] <token>")
		os.Exit(1)
	}

	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		fmt.Fprintln(os.Stderr, "Error: invalid JWT format")
		os.Exit(1)
	}

	claimsJSON, err := auth.B64Decode(parts[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding token: %v\n", err)
		os.Exit(1)
	}

	var claims auth.Claims
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing claims: %v\n", err)
		os.Exit(1)
	}

	if *apiKey != "" && *apiSecret != "" {
		v := auth.NewTokenVerifier(*apiKey, *apiSecret)
		if _, err := v.Verify(tokenStr); err != nil {
			fmt.Fprintf(os.Stderr, "Verification: FAILED (%v)\n", err)
		} else {
			fmt.Fprintln(os.Stderr, "Verification: OK")
		}
	}

	fmt.Printf("Identity:  %s\n", claims.Sub)
	fmt.Printf("Name:      %s\n", claims.Name)
	fmt.Printf("Issuer:    %s\n", claims.Iss)
	fmt.Printf("TokenID:   %s\n", claims.Jti)
	fmt.Printf("IssuedAt:  %s\n", time.Unix(claims.Iat, 0).Format(time.RFC3339))
	fmt.Printf("NotBefore: %s\n", time.Unix(claims.Nbf, 0).Format(time.RFC3339))
	fmt.Printf("ExpiresAt: %s\n", time.Unix(claims.Exp, 0).Format(time.RFC3339))

	now := time.Now()
	if claims.Exp > 0 && claims.Exp > now.Unix() {
		fmt.Printf("Expires in: %s\n", time.Until(time.Unix(claims.Exp, 0)).Truncate(time.Second))
	} else if claims.Exp > 0 {
		fmt.Printf("Expired:   %s ago\n", time.Since(time.Unix(claims.Exp, 0)).Truncate(time.Second))
	}

	if claims.Grants != nil {
		pretty, _ := json.MarshalIndent(claims.Grants, "", "  ")
		fmt.Printf("Grants:\n%s\n", pretty)
	}
}
