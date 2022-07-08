package indexer

import (
	"errors"
	"math/big"

	"github.com/pokt-foundation/pocket-go/provider"
)

var (
	// ErrNoAppsToIndex error when there are no apps to index
	ErrNoAppsToIndex = errors.New("no apps to index")
)

// App struct handler of all app fields to be indexed
type App struct {
	Address      string
	Height       int
	Jailed       bool
	PublicKey    string
	StakedTokens *big.Int
}

func convertProviderAppToApp(height int, provApp *provider.App) *App {
	stakedTokens := new(big.Int)
	stakedTokens, _ = stakedTokens.SetString(provApp.StakedTokens, 10)

	return &App{
		Address:      provApp.Address,
		Height:       height,
		Jailed:       provApp.Jailed,
		PublicKey:    provApp.PublicKey,
		StakedTokens: stakedTokens,
	}
}

// IndexBlockApps converts apps details to known structures and saved them
// returns all addresses indexed
func (i *Indexer) IndexBlockApps(blockHeight int) ([]string, error) {
	totalPages := 1
	var providerApps []*provider.App

	for page := 1; page <= totalPages; page++ {
		appsOutput, err := i.provider.GetApps(&provider.GetAppsOptions{
			Height:  blockHeight,
			Page:    page,
			PerPage: 10000,
		})
		if err != nil {
			return nil, err
		}

		if page == 1 {
			totalPages = appsOutput.TotalPages
		}

		providerApps = append(providerApps, appsOutput.Result...)
	}

	if len(providerApps) == 0 {
		return nil, ErrNoAppsToIndex
	}

	var apps []*App
	var addresses []string

	for _, app := range providerApps {
		apps = append(apps, convertProviderAppToApp(blockHeight, app))
		addresses = append(addresses, app.Address)
	}

	return addresses, i.writer.WriteApps(apps)
}
