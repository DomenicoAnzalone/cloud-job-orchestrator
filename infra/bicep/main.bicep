@description('Deployment environment suffix used for naming and tags (e.g. dev, test, prod).')
param environment string = 'dev'

@description('Azure location for all resources.')
param location string = resourceGroup().location

@description('Base prefix used for globally-unique resource names. Use lowercase letters and numbers only.')
@minLength(3)
@maxLength(12)
param namePrefix string

@description('Service Bus queue name used by the backend worker trigger.')
param serviceBusQueueName string = 'q-jobs'

@description('Cosmos DB SQL database name.')
param cosmosDatabaseName string = 'cjo'

@description('Cosmos DB SQL container name.')
param cosmosContainerName string = 'jobs'

@description('Cosmos DB partition key path.')
param cosmosPartitionKeyPath string = '/pk'

@description('SignalR hub name used by the backend negotiate/publish endpoints.')
param signalrHubName string = 'jobs'

@description('Output blob container name.')
param blobOutputContainerName string = 'output'

@description('Input blob container name.')
param blobInputContainerName string = 'input'

@description('App Service plan SKU for frontend Web App (e.g. B1, P1v3).')
param webPlanSkuName string = 'B1'

@description('App Service plan tier for frontend Web App.')
param webPlanSkuTier string = 'Basic'

@description('Application Insights daily data cap in GB.')
param appInsightsDailyCapGb int = 5

@description('Entra tenant ID used by frontend and backend settings.')
param azureAdTenantId string = '<set-manually>'

@description('Frontend app registration client ID used by MSAL.')
param azureAdClientId string = '<set-manually>'

@description('Backend API audience / Application ID URI (example: api://<api-app-id>).')
param azureAdApiAudience string = '<set-manually>'

@description('OAuth scope requested by frontend (example: api://<api-app-id>/access_as_user).')
param azureAdApiScope string = '<set-manually>'

var suffix = uniqueString(subscription().id, resourceGroup().id, namePrefix, environment)
var baseName = replace('${namePrefix}${environment}${suffix}', '-', '')
var storageAccountName = toLower(take(baseName, 24))
var serviceBusNamespaceName = toLower(take(replace('${namePrefix}-${environment}-sb-${suffix}', '-', ''), 50))
var cosmosAccountName = toLower(take(replace('${namePrefix}-${environment}-cosmos-${suffix}', '-', ''), 44))
var signalrName = toLower(take(replace('${namePrefix}-${environment}-signalr-${suffix}', '-', ''), 63))
var appInsightsName = '${namePrefix}-${environment}-appi'
var functionPlanName = '${namePrefix}-${environment}-plan-func'
var functionAppName = '${namePrefix}-${environment}-func-${take(suffix, 6)}'
var webPlanName = '${namePrefix}-${environment}-plan-web'
var webAppName = '${namePrefix}-${environment}-web-${take(suffix, 6)}'

var tags = {
  app: 'cloud-job-orchestrator'
  environment: environment
  provisioner: 'bicep'
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: 'norwayeast'
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    accessTier: 'Hot'
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource inputContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: blobInputContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource outputContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: blobOutputContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource serviceBus 'Microsoft.ServiceBus/namespaces@2023-01-01-preview' = {
  name: serviceBusNamespaceName
  location: location
  tags: tags
  sku: {
    name: 'Standard'
    tier: 'Standard'
  }
  properties: {
    minimumTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}


resource serviceBusRootAuthRule 'Microsoft.ServiceBus/namespaces/AuthorizationRules@2023-01-01-preview' existing = {
  parent: serviceBus
  name: 'RootManageSharedAccessKey'
}

resource jobsQueue 'Microsoft.ServiceBus/namespaces/queues@2023-01-01-preview' = {
  parent: serviceBus
  name: serviceBusQueueName
  properties: {
    maxDeliveryCount: 10
    deadLetteringOnMessageExpiration: true
    lockDuration: 'PT1M'
    defaultMessageTimeToLive: 'P14D'
  }
}

resource cosmos 'Microsoft.DocumentDB/databaseAccounts@2024-05-15' = {
  name: cosmosAccountName
  location: location
  tags: tags
  kind: 'GlobalDocumentDB'
  properties: {
    databaseAccountOfferType: 'Standard'
    publicNetworkAccess: 'Enabled'
    disableLocalAuth: false
    minimalTlsVersion: 'Tls12'
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
    locations: [
      {
        locationName: location
        failoverPriority: 0
        isZoneRedundant: false
      }
    ]
    capabilities: [
      {
        name: 'EnableServerless'
      }
    ]
  }
}

resource cosmosSqlDb 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2024-05-15' = {
  parent: cosmos
  name: cosmosDatabaseName
  properties: {
    resource: {
      id: cosmosDatabaseName
    }
  }
}

resource cosmosSqlContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-05-15' = {
  parent: cosmosSqlDb
  name: cosmosContainerName
  properties: {
    resource: {
      id: cosmosContainerName
      partitionKey: {
        paths: [
          cosmosPartitionKeyPath
        ]
        kind: 'Hash'
      }
      indexingPolicy: {
        indexingMode: 'consistent'
        automatic: true
        includedPaths: [
          {
            path: '/*'
          }
        ]
        excludedPaths: [
          {
            path: '/"_etag"/?'
          }
        ]
      }
    }
  }
}

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: appInsightsName
  location: location
  kind: 'web'
  tags: tags
  properties: {
    Application_Type: 'web'
    RetentionInDays: 30
    IngestionMode: 'ApplicationInsights'
  }
}

resource signalr 'Microsoft.SignalRService/signalR@2023-02-01' = {
  name: signalrName
  location: location
  tags: tags
  sku: {
    name: 'Standard_S1'
    capacity: 1
  }
  kind: 'SignalR'
  properties: {
    features: [
      {
        flag: 'ServiceMode'
        value: 'Default'
      }
    ]
    tls: {
      clientCertEnabled: false
    }
    publicNetworkAccess: 'Enabled'
  }
}


resource signalrPrimaryKey 'Microsoft.SignalRService/signalR@2023-02-01' existing = {
  name: signalr.name
}

resource functionPlan 'Microsoft.Web/serverfarms@2023-12-01' = {
  name: functionPlanName
  location: location
  kind: 'functionapp'
  tags: tags
  sku: {
    name: 'Y1'
    tier: 'Dynamic'
  }
  properties: {
    reserved: false
  }
}

resource functionApp 'Microsoft.Web/sites@2023-12-01' = {
  name: functionAppName
  location: location
  kind: 'functionapp'
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: functionPlan.id
    httpsOnly: true
    clientAffinityEnabled: false
    siteConfig: {
      minTlsVersion: '1.2'
      ftpsState: 'Disabled'
      appSettings: [
        {
          name: 'FUNCTIONS_EXTENSION_VERSION'
          value: '~4'
        }
        {
          name: 'FUNCTIONS_WORKER_RUNTIME'
          value: 'python'
        }
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storage.name};AccountKey=${storage.listKeys().keys[0].value};EndpointSuffix=${az.environment().suffixes.storage}'
        }
        {
          name: 'DEPLOYMENT_STORAGE_CONNECTION_STRING'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storage.name};AccountKey=${storage.listKeys().keys[0].value};EndpointSuffix=${az.environment().suffixes.storage}'
        }
        {
          name: 'SERVICEBUS_CONNECTION'
          value: listKeys(serviceBusRootAuthRule.id, serviceBusRootAuthRule.apiVersion).primaryConnectionString
        }
        {
          name: 'SERVICEBUS_JOBS_QUEUE'
          value: serviceBusQueueName
        }
        {
          name: 'COSMOS_ENDPOINT'
          value: cosmos.properties.documentEndpoint
        }
        {
          name: 'COSMOS_KEY'
          value: cosmos.listKeys().primaryMasterKey
        }
        {
          name: 'COSMOS_DB'
          value: cosmosDatabaseName
        }
        {
          name: 'COSMOS_CONTAINER'
          value: cosmosContainerName
        }
        {
          name: 'BLOB_CONNECTION'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storage.name};AccountKey=${storage.listKeys().keys[0].value};EndpointSuffix=${az.environment().suffixes.storage}'
        }
        {
          name: 'BLOB_ACCOUNT_URL'
          value: storage.properties.primaryEndpoints.blob
        }
        {
          name: 'BLOB_OUTPUT_CONTAINER'
          value: blobOutputContainerName
        }
        {
          name: 'BLOB_INPUT_CONTAINER'
          value: blobInputContainerName
        }
        {
          name: 'OUTPUT_LINK_TTL_MINUTES'
          value: '10'
        }
        {
          name: 'SIGNALR_CONNECTION_STRING'
          value: listKeys(signalrPrimaryKey.id, signalrPrimaryKey.apiVersion).primaryConnectionString
        }
        {
          name: 'SIGNALR_HUB_NAME'
          value: signalrHubName
        }
        {
          name: 'FRONTEND_API_BASE_URL'
          value: 'https://${functionAppName}.azurewebsites.net/api'
        }
        {
          name: 'AZURE_AD_TENANT_ID'
          value: azureAdTenantId
        }
        {
          name: 'AZURE_AD_CLIENT_ID'
          value: azureAdClientId
        }
        {
          name: 'AZURE_AD_API_AUDIENCE'
          value: azureAdApiAudience
        }
        {
          name: 'AZURE_AD_API_SCOPE'
          value: azureAdApiScope
        }
        {
          name: 'AZURE_AD_AUTHORITY'
          value: '${az.environment().authentication.loginEndpoint}${azureAdTenantId}'
        }
        {
          name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
          value: appInsights.properties.ConnectionString
        }
      ]
    }
  }
  dependsOn: [
    inputContainer
    outputContainer
    jobsQueue
    cosmosSqlContainer
  ]
}

resource webPlan 'Microsoft.Web/serverfarms@2023-12-01' = {
  name: webPlanName
  location: location
  kind: 'linux'
  tags: tags
  sku: {
    name: webPlanSkuName
    tier: webPlanSkuTier
  }
  properties: {
    reserved: true
  }
}

resource webApp 'Microsoft.Web/sites@2023-12-01' = {
  name: webAppName
  location: location
  kind: 'app,linux'
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: webPlan.id
    httpsOnly: true
    clientAffinityEnabled: false
    siteConfig: {
      linuxFxVersion: 'NODE|20-lts'
      minTlsVersion: '1.2'
      ftpsState: 'Disabled'
      appSettings: [
        {
          name: 'SCM_DO_BUILD_DURING_DEPLOYMENT'
          value: 'true'
        }
        {
          name: 'WEBSITE_RUN_FROM_PACKAGE'
          value: '1'
        }
        {
          name: 'PORT'
          value: '8080'
        }
        {
          name: 'FRONTEND_API_BASE_URL'
          value: 'https://${functionAppName}.azurewebsites.net/api'
        }
        {
          name: 'API_BASE_URL'
          value: 'https://${functionAppName}.azurewebsites.net/api'
        }
        {
          name: 'CJO_API_BASE_URL'
          value: 'https://${functionAppName}.azurewebsites.net/api'
        }
        {
          name: 'AZURE_AD_TENANT_ID'
          value: azureAdTenantId
        }
        {
          name: 'AZURE_AD_CLIENT_ID'
          value: azureAdClientId
        }
        {
          name: 'AZURE_AD_API_AUDIENCE'
          value: azureAdApiAudience
        }
        {
          name: 'AZURE_AD_API_SCOPE'
          value: azureAdApiScope
        }
        {
          name: 'AZURE_AD_AUTHORITY'
          value: 'https://login.microsoftonline.com/${azureAdTenantId}'
        }
        {
          name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
          value: appInsights.properties.ConnectionString
        }
      ]
    }
  }
  dependsOn: [
    functionApp
  ]
}

resource roleStorageFunction 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, functionApp.id, 'StorageBlobDataContributor')
  scope: storage
  properties: {
    principalId: functionApp.identity.principalId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalType: 'ServicePrincipal'
  }
}

resource roleStorageWeb 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, webApp.id, 'StorageBlobDataContributor')
  scope: storage
  properties: {
    principalId: webApp.identity.principalId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalType: 'ServicePrincipal'
  }
}

resource roleCosmosFunction 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-05-15' = {
  name: guid(cosmos.id, functionApp.id, 'CosmosDataContributor')
  parent: cosmos
  properties: {
    principalId: functionApp.identity.principalId
    roleDefinitionId: '${cosmos.id}/sqlRoleDefinitions/00000000-0000-0000-0000-000000000002'
    scope: cosmos.id
  }
}

resource roleServiceBusFunctionSender 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(serviceBus.id, functionApp.id, 'ServiceBusDataSender')
  scope: serviceBus
  properties: {
    principalId: functionApp.identity.principalId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '69a216fc-b8fb-44d8-bc22-1f3c2cd27a39')
    principalType: 'ServicePrincipal'
  }
}

resource roleServiceBusFunctionReceiver 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(serviceBus.id, functionApp.id, 'ServiceBusDataReceiver')
  scope: serviceBus
  properties: {
    principalId: functionApp.identity.principalId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '090c5cfd-751d-490a-894a-3ce6f1109419')
    principalType: 'ServicePrincipal'
  }
}

resource roleSignalRFunction 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(signalr.id, functionApp.id, 'SignalRAppServer')
  scope: signalr
  properties: {
    principalId: functionApp.identity.principalId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '420fcaa2-552c-430f-98ca-3264be4806c7')
    principalType: 'ServicePrincipal'
  }
}

output resourceGroupName string = resourceGroup().name
output locationUsed string = location

output storageAccountName string = storage.name
output inputContainer string = inputContainer.name
output outputContainer string = outputContainer.name

output serviceBusNamespace string = serviceBus.name
output serviceBusQueue string = jobsQueue.name

output cosmosAccountName string = cosmos.name
output cosmosEndpoint string = cosmos.properties.documentEndpoint
output cosmosDatabase string = cosmosDatabaseName
output cosmosContainer string = cosmosContainerName

output functionAppName string = functionApp.name
output functionAppUrl string = 'https://${functionApp.properties.defaultHostName}'

output webAppName string = webApp.name
output webAppUrl string = 'https://${webApp.properties.defaultHostName}'

output signalrName string = signalr.name
output signalrEndpoint string = 'https://${signalr.name}.service.signalr.net'

output appInsightsName string = appInsights.name
output appInsightsConnectionString string = appInsights.properties.ConnectionString

output managedIdentityPrincipalIdFunction string = functionApp.identity.principalId
output managedIdentityPrincipalIdWeb string = webApp.identity.principalId
