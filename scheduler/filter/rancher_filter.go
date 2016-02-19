package filter

import (
	"os"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/swarm/cluster"
	"github.com/docker/swarm/scheduler/node"
	"github.com/rancher/go-rancher/client"
)

const (
	MAX_WAIT    = time.Duration(time.Second * 10)
)
var (
	rancherClient *client.RancherClient
	cattleUrl, projectsUrl, cattleApiKey, cattleSecretKey string
	initialized = false
	rancherIPAddressHostIDMap map[string]string
)

func init() {
	//get the cattle keys
	cattleUrl = os.Getenv("CATTLE_URL")
	if len(cattleUrl) == 0 {
		log.Errorf("CATTLE_URL is not set, cannot init rancher filter")
	}
	projectsUrl = cattleUrl + "/projects/1a5/schemas"

	cattleApiKey = os.Getenv("CATTLE_ACCESS_KEY")
	if len(cattleApiKey) == 0 {
		log.Errorf("CATTLE_ACCESS_KEY is not set, cannot init rancher filter")
	}

	cattleSecretKey = os.Getenv("CATTLE_SECRET_KEY")
	if len(cattleSecretKey) == 0 {
		log.Errorf("CATTLE_SECRET_KEY is not set, cannot init rancher filter")
	}

	var err error
	//configure cattle client
	rancherClient, err = newCattleClient(cattleUrl, cattleApiKey, cattleSecretKey)
	if err != nil {
		log.Errorf("Failed to configure rancher client: %v, cannot init rancher filter", err)
	}

	err = TestConnect()
	if err != nil {
		log.Errorf("Failed to connect to rancher cattle api: %v, cannot init rancher filter", err)
	}
	
	//build the rancher hostId cache
	rancherIPAddressHostIDMap = make(map[string]string) 
	err = buildRancherIPAddressHostIDMap()
	if err != nil {
		log.Errorf("Failed to connect to rancher to list the hosts: %v, cannot init rancher filter", err)
	}
	//start ticker to periodically refresh the hostId cache
	
	startPeriodicHostMapRefresh()
	
	initialized = true
}

// RancherFilter selects nodes that rancher scheduler finds fit.
type RancherFilter struct {
}

// Name returns the name of the filter
func (f *RancherFilter) Name() string {
	return "rancher"
}

// Filter is exported
func (f *RancherFilter) Filter(config *cluster.ContainerConfig, nodes []*node.Node, soft bool) ([]*node.Node, error) {
	
	if !initialized {
		log.Errorf("Init failed for rancher_filter, cannot filter the nodes")
		return nodes, nil
	}
	
	log.Debugf("Called Rancher filter")
	
	log.Debug("nodes are these:  %v", nodes)
	
	var selectedNodes []*node.Node
	var rancherSelectedIpAddress string
	
	apiClient, err := newCattleClient(projectsUrl, cattleApiKey, cattleSecretKey)
	if err != nil {
		log.Errorf("Failed to configure rancher client: %v, cannot init rancher filter", err)
	}

	networkMode := config.ContainerConfig.HostConfig.NetworkMode
	if strings.EqualFold("default", networkMode) {
		networkMode = "bridge"
	}
	
	rancherLabelMap := make(map[string]interface{})
	for key, value := range config.ContainerConfig.Labels {
		rancherLabelMap[key] = value
		if strings.EqualFold(key, "io.rancher.container.network"){
			if strings.EqualFold(value, "true") {
				networkMode = "managed"
			}
		}
	}

	validHostIds := getValidRancherHosts(nodes)
	log.Debugf("validHostIds %v",validHostIds)
	
	container, err := apiClient.Container.Create(&client.Container{
		ImageUuid: "docker:" + config.ContainerConfig.Image,
		Labels: rancherLabelMap,
		NetworkMode: networkMode,
		StartOnCreate: false,
		NativeContainer: true,
		ValidHostIds: validHostIds,
	})
	if err != nil {
		log.Error("rancher_filter cannot filter the nodes, error: %v", err)
		return nodes, nil
	}

	//add a rancher label
	config.ContainerConfig.Labels["io.rancher.container.uuid"] = container.Uuid
	
	container = waitContainerTransition(container, rancherClient)
	if container.State == "inactive" {
		log.Error("container shouldnt be inactive.")
	}
	
	var hosts client.HostCollection
	
	err = rancherClient.GetLink(container.Resource, "hosts", &hosts)
	if err != nil {
		log.Error("rancher_filter cannot filter the nodes, error: %v", err)
		return nodes, nil
	} 
	
	for _, host := range hosts.Data {
		//log.Debugf("rancher container host:  %v", host)
		
		var ipAddresses client.IpAddressCollection
		err = rancherClient.GetLink(host.Resource, "ipAddresses", &ipAddresses)
		if err != nil {
			log.Error("rancher_filter cannot filter the nodes, error: %v", err)
			return nodes, nil
		}
		
		for _, ipAddress := range ipAddresses.Data {
			//log.Debugf("rancher container hostIP:  %v", ipAddress)
			rancherSelectedIpAddress = ipAddress.Address
		}
	} 

	for _, swarmNode := range nodes {	
		if strings.EqualFold(swarmNode.IP, rancherSelectedIpAddress) {
			 //log.Debugf("Found Swarm Node match:  %v", swarmNode)
			 selectedNodes = append(selectedNodes, swarmNode)
		}
	}
	
	log.Debug("selectedNodes are these:  %v", selectedNodes)
	
	return selectedNodes, nil
}

func getValidRancherHosts(nodes []*node.Node) []string {
	var validHostIds []string
	
	for _, node := range nodes {
		hostId, ok := rancherIPAddressHostIDMap[node.IP]
		if ok {
			validHostIds = append(validHostIds, hostId)
		}
	}
	
	return validHostIds
}


func newCattleClient(cattleUrl string, cattleAccessKey string, cattleSecretKey string) (*client.RancherClient, error) {
	apiClient, err := client.NewRancherClient(&client.ClientOpts{
		Url:       cattleUrl,
		AccessKey: cattleAccessKey,
		SecretKey: cattleSecretKey,
	})

	if err != nil {
		return nil, err
	}

	return apiClient, nil
}

func TestConnect() error {
	opts := &client.ListOpts{}
	_, err := rancherClient.ContainerEvent.List(opts)
	return err
}

func startPeriodicHostMapRefresh() {
	ticker := time.NewTicker(time.Duration(300) * time.Second)
	go func() {
		for _ = range ticker.C {
			buildRancherIPAddressHostIDMap()
		}
	}()
}

func buildRancherIPAddressHostIDMap() error {
	log.Debug("Listing Rancher Hosts and building host iPAddress-hostId map")
	//list rancher hosts
	apiClient, err := newCattleClient(projectsUrl, cattleApiKey, cattleSecretKey)
	if err != nil {
		log.Errorf("Failed to configure rancher client: %v", err)
		return err
	}

	hosts, err := apiClient.Host.List(nil)
	
	for _, host := range hosts.Data {
		var ipAddresses client.IpAddressCollection
		err = rancherClient.GetLink(host.Resource, "ipAddresses", &ipAddresses)
		if err != nil {
			log.Error("Failed to list host's ip_address, for host %v, error: %v", host.Id, err)
			continue
		}
		for _, ipAddress := range ipAddresses.Data {
			rancherIPAddressHostIDMap[ipAddress.Address] = host.Id
		}
	}
	
	return nil
}


func waitContainerTransition(container *client.Container, client *client.RancherClient) *client.Container {
	timeoutAt := time.Now().Add(MAX_WAIT)
	ticker := time.NewTicker(time.Millisecond * 250)
	defer ticker.Stop()
	for tick := range ticker.C {
		container, err := client.Container.ById(container.Id)
		if err != nil {
			log.Error("Couldn't get container")
		}
		if container.Transitioning != "yes" {
			return container
		}
		if tick.After(timeoutAt) {
			log.Error("Timed out waiting for container to activate.")
		}
	}
	log.Error("Timed out waiting for container to activate.")
	return nil
}
