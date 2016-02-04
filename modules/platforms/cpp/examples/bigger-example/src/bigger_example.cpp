/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <ctime>

#include <iostream>
#include <map>

#include <boost/random.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

#include "ignite/examples/city.h"
#include "ignite/examples/client.h"
#include "ignite/examples/account.h"
#include "ignite/examples/product.h"
#include "ignite/examples/client_product_relation.h"

using namespace ignite;
using namespace cache;

using namespace examples;

std::time_t now = std::time(0);
boost::random::mt19937 gen(static_cast<uint32_t>(now));

/**
 * Fill collection with test cities data.
 *
 * @param cities Collection to be filled.
 */
void GenerateCities(std::map<int64_t, City>& cities)
{
    cities.clear();

    int64_t idCounter = 0;

    cities[idCounter] = City(++idCounter, "New York",      8491079);
    cities[idCounter] = City(++idCounter, "Los Angeles",   3884307);
    cities[idCounter] = City(++idCounter, "Chicago",       2722389);
    cities[idCounter] = City(++idCounter, "Houston",       2239558);
    cities[idCounter] = City(++idCounter, "Philadelphia",  1560297);
    cities[idCounter] = City(++idCounter, "San Diego",     1381069);
    cities[idCounter] = City(++idCounter, "San Francisco", 852469);
    cities[idCounter] = City(++idCounter, "Charlotte",     809958);
    cities[idCounter] = City(++idCounter, "Detroit",       680250);
    cities[idCounter] = City(++idCounter, "El Paso",       679036);
    cities[idCounter] = City(++idCounter, "Seattle",       668342);
    cities[idCounter] = City(++idCounter, "Denver",        663862);
    cities[idCounter] = City(++idCounter, "Boston",        655884);
    cities[idCounter] = City(++idCounter, "Las Vegas",     613599);
    cities[idCounter] = City(++idCounter, "Atlanta",       456002);
    cities[idCounter] = City(++idCounter, "Omaha",         446599);
    cities[idCounter] = City(++idCounter, "Miami",         430332);
}

/**
 * Fill collection with test client data.
 *
 * @param clients Clients collection to be filled.
 * @param cities Filled cities collection to use.
 * @param num Number of clients to generate.
 */
void GenerateClients(std::map<int64_t, Client>& clients,
    const std::map<int64_t, City>& cities, int64_t num)
{
    clients.clear();

    int64_t idCounter = 0;

    std::vector<std::string> firstNames;

    firstNames.push_back("Emily");
    firstNames.push_back("Amelia");
    firstNames.push_back("Olivia");
    firstNames.push_back("Jessica");
    firstNames.push_back("Lily");
    firstNames.push_back("Sophie");
    firstNames.push_back("Grace");
    firstNames.push_back("Mia");
    firstNames.push_back("Evie");
    firstNames.push_back("Ruby");
    firstNames.push_back("Chloe");
    firstNames.push_back("Scarlett");
    firstNames.push_back("Phoebe");
    firstNames.push_back("Charlotte");
    firstNames.push_back("Daisy");
    firstNames.push_back("Alice");
    firstNames.push_back("Sofia");
    firstNames.push_back("Florence");
    firstNames.push_back("Oliver");
    firstNames.push_back("Jack");
    firstNames.push_back("Harry");
    firstNames.push_back("Jacob");
    firstNames.push_back("Charlie");
    firstNames.push_back("Thomas");
    firstNames.push_back("George");
    firstNames.push_back("Oscar");
    firstNames.push_back("James");
    firstNames.push_back("William");
    firstNames.push_back("Henry");
    firstNames.push_back("Muhamma");
    firstNames.push_back("Joseph");
    firstNames.push_back("Daniel");
    firstNames.push_back("Edward");
    firstNames.push_back("Jake");
    firstNames.push_back("David");

    std::vector<std::string> lastNames;

    lastNames.push_back("Smith");
    lastNames.push_back("Jones");
    lastNames.push_back("Taylor");
    lastNames.push_back("Williams");
    lastNames.push_back("Brown");
    lastNames.push_back("Davies");
    lastNames.push_back("Evans");
    lastNames.push_back("Wilson");
    lastNames.push_back("Thomas");
    lastNames.push_back("Robert");
    lastNames.push_back("Johnson");
    lastNames.push_back("Lewis");
    lastNames.push_back("Walker");

    int64_t allPopulation = 0;

    std::map<int64_t, City>::const_iterator it;
    for (it = cities.begin(); it != cities.end(); ++it)
        allPopulation += it->second.population;

    // Selecting random city considering its weight - population.
    boost::random::uniform_int_distribution<int64_t> cityPopulationDistr(0, allPopulation - 1);

    // First name distribution.
    boost::random::uniform_int_distribution<size_t> firstNameDistr(0, firstNames.size() - 1);

    // Last name distribution.
    boost::random::uniform_int_distribution<size_t> lastNameDistr(0, lastNames.size() - 1);

    for (int64_t i = 1; i <= num; ++i)
    {
        int64_t cityId = 0;

        // Generating cityId depending on city weight - population.
        int64_t populationPos = cityPopulationDistr(gen);

        for (it = cities.begin(); it != cities.end(); ++it)
        {
            populationPos -= it->second.population;

            if (populationPos < 0)
            {
                cityId = it->first;

                break;
            }
        }

        size_t firstNameIdx = firstNameDistr(gen);
        size_t lastNameIdx = lastNameDistr(gen);

        clients[i] = Client(i, cityId, firstNames[firstNameIdx], lastNames[lastNameIdx]);
    }
}

/**
 * Fill collection with test account data.
 *
 * @param accounts Accounts collection to be filled.
 * @param clients Filled clients collection to use.
 * @param avrgNum Average number of accounts per client.
 */
void GenerateAccounts(std::map<int64_t, Account>& accounts,
    const std::map<int64_t, Client>& clients, double accountCntScale, double balanceScale)
{
    accounts.clear();

    // Account number per client distributed by exponential law.
    boost::random::exponential_distribution<double> accountCntDistr;

    // Generating accounts for clients.
    std::map<int64_t, Client>::const_iterator itc;
    for (itc = clients.begin(); itc != clients.end(); ++itc)
    {
        // Generating number of accounts for client using exponentional
        // distribution. Minimum is 1.
        int accountsCnt = 1 + static_cast<int>(accountCntDistr(gen) * accountCntScale);

        // Generating acounts for the client.
        for (int i = 0; i < accountsCnt; ++i)
        {
            int64_t accountId = static_cast<int64_t>(accounts.size() + 1);

            // Do not generate ballance just now.
            accounts[accountId] = Account(accountId, itc->first, 0.0);
        }
    }

    // Using log-normal distribution for balance.
    boost::random::lognormal_distribution<double> balanceDistr;

    // Generating balance for accounts.
    std::map<int64_t, Account>::iterator ita;
    for (ita = accounts.begin(); ita != accounts.end(); ++ita)
        ita->second.balance = balanceDistr(gen) * balanceScale;
}

/**
 * Fill collection with test product data.
 *
 * @param products Product collection to be filled.
 * @param num Number of products to generate.
 */
void GenerateProducts(std::map<int64_t, Product>& products, int64_t num)
{
    products.clear();

    // Using uniform distribution for price (hundreds of currency).
    boost::random::uniform_int_distribution<int8_t> priceDistr(1, 100);

    // Generating products.
    for (int64_t i = 1; i <= num; ++i)
    {
        std::stringstream ss("Product");
        ss << i;

        double price = priceDistr(gen) * 100.0;

        products[i] = Product(i, ss.str(), price);
    }
}


/**
 * Fill collection with test client-product relation data.
 *
 * @param clientProductRels ClientProductRelation collection to be filled.
 * @param num Number of relations to generate.
 */
void GenerateClientProductRelations(std::map<int64_t, ClientProductRelation>& clientProductRels,
    const std::map<int64_t, Client>& clients, const std::map<int64_t, Product>& products, int64_t num)
{
    clientProductRels.clear();

    // Starting with generating weights for the products - their popularity.
    std::map<int64_t, int8_t> productPopularity;

    // Using uniform distribution.
    boost::random::uniform_int_distribution<int8_t> productPopularityDistr(5, 100);

    // We will need sum later.
    int64_t popularitySum = 0;

    std::map<int64_t, Product>::const_iterator it;
    for (it = products.begin(); it != products.end(); ++it)
    {
        int8_t popularity = productPopularityDistr(gen);

        popularitySum += popularity;

        productPopularity[it->first] = popularity;
    }

    // Distributing products between clients using uniform distribution
    // in range [1..clientsNum].
    boost::random::uniform_int_distribution<int64_t> clientIdDistr(1, clients.size() + 1);

    // We are going to generate values in this range to select product
    // considering its popularity.
    boost::random::uniform_int_distribution<int64_t> popularityRangeDistr(1, popularitySum);

    // Generating relations.
    for (int64_t i = 1; i <= num; ++i)
    {
        // Generating client id.
        int64_t clientId = clientIdDistr(gen);

        // Shooting in popularity range.
        int64_t popularity = popularityRangeDistr(gen);

        int64_t productId = 1;

        // Finding out product id.
        std::map<int64_t, int8_t>::const_iterator pit;
        for (pit = productPopularity.begin(); pit != productPopularity.end(); ++pit)
        {
            popularity -= pit->second;

            if (popularity <= 0)
            {
                productId = pit->first;

                break;
            }
        }

        int64_t clientProductRelId = clientProductRels.size() + 1;

        clientProductRels[clientProductRelId] = ClientProductRelation(clientProductRelId, clientId, productId);
    }
}

/**
 * Populate caches with generated data.
 */
void Populate(Ignite& grid)
{
    std::map<int64_t, City> cities;
    std::map<int64_t, Client> clients;
    std::map<int64_t, Account> accounts;
    std::map<int64_t, Product> products;
    std::map<int64_t, ClientProductRelation> clientProducts;

    // Generating test data.
    GenerateCities(cities);
    GenerateClients(clients, cities, 10000);
    GenerateAccounts(accounts, clients, 1.0, 50000.0);
    GenerateProducts(products, 24);
    GenerateClientProductRelations(clientProducts, clients, products, 20000);

    // Getting cache instances.
    Cache<int64_t, City> cityCache = grid.GetCache<int64_t, City>("City");
    Cache<int64_t, Client> clientCache = grid.GetCache<int64_t, Client>("Client");
    Cache<int64_t, Account> accountCache = grid.GetCache<int64_t, Account>("Account");
    Cache<int64_t, Product> productCache = grid.GetCache<int64_t, Product>("Product");
    Cache<int64_t, ClientProductRelation> clientProductCache = grid.GetCache<int64_t, ClientProductRelation>("ClientProductRelation");

    // Clearing caches.
    cityCache.Clear();
    clientCache.Clear();
    accountCache.Clear();
    productCache.Clear();
    clientProductCache.Clear();

    // Loading entries into caches.
    cityCache.PutAll(cities);
    clientCache.PutAll(clients);
    accountCache.PutAll(accounts);
    productCache.PutAll(products);
    clientProductCache.PutAll(clientProducts);
}

/**
 * Program entry point.
 *
 * @return Exit code.
 */
int main() 
{
    IgniteConfiguration cfg;

    cfg.jvmInitMem = 512;
    cfg.jvmMaxMem = 512;

    cfg.springCfgPath = "platforms/cpp/examples/bigger-example/config/example-cache.xml";

    try
    {
        // Start a node.
        Ignite grid = Ignition::Start(cfg);

        std::cout << std::endl;
        std::cout << ">>> Cache bigger example started." << std::endl;
        std::cout << std::endl;

        std::cout << ">>> Generating example data..." << std::endl;

        Populate(grid);

        std::cout << ">>> Data is generated and loaded to cache successfully." << std::endl;

        std::cout << ">>> Example is running, press any key to stop ..." << std::endl;
        std::cin.get();

        // Stop node.
        Ignition::StopAll(false);
    }
    catch (IgniteError& err)
    {
        std::cout << "An error occurred: " << err.GetText() << std::endl;
    }

    std::cout << std::endl;
    std::cout << ">>> Example finished, exiting." << std::endl;
    std::cout << std::endl;

    return 0;
}