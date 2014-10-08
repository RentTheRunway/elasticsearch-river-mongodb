/**
 * Created by emclean on 9/29/14.
 */


@Grab(group = 'org.codehaus.groovy.modules.http-builder', module = 'http-builder', version = '0.7.1')
@Grab(group = 'commons-cli', module = 'commons-cli', version = '1.2')
@GrabExclude('xerces:xercesImpl')

import groovyx.net.http.RESTClient
import static groovyx.net.http.ContentType.*
import groovy.transform.Field
import groovy.json.JsonBuilder

//These can change from env to env
@Field def esHost = "localhost"

//Set these based on the mongoInstance arg
@Field def mongoServers
@Field def mongoDb

//These shouldn't change from env to env
@Field def mongoAuthDb = "local"
@Field def oplogDb = "local"
@Field def oplogUser = "oplog"
@Field def oplogPass = "weReadOpl0gs"
@Field def collections = ["Accessory", "Dress", "Designer", "SaleableProduct"]
@Field RESTClient es

run(args)

/**
 * Run/Main method
 */
void run(args) {
    init(args)
    deleteIndices()
    createCatalogIndex()
    createCollectionRivers()
}

/**
 * Initialize and validate the ES client
 */
void init(args) {
    parseArgs(args)

    println "initializing the es client"
    es = new RESTClient("http://${esHost}:9200/")
    def resp = es.get(path: "/_cluster/health")
    assert (resp.status == 200)
}

void parseArgs(args) {
    println "parsing the command line args"
    def cli = new CliBuilder(usage: 'groovy setupRiver.groovy [-h] -mi [prod|dev] -eh [ES_HOST] ')
    cli.h(longOpt: 'help', 'usage information', required: false)
    cli.mi(longOpt: 'mongoInstance', 'must be set to prod or dev, if prod we read from instance01 productcatalog_prod if dev instance01Dev productcatalog_stage', required: true, args: 1)
    cli.eh(longOpt: 'esHost', 'The elastic search host, e.g. es.s.rtrdc.net, defaults to localhost if not specified', required: false, args: 1)
    def opt = cli.parse(args)

    if (!opt || opt.h || !opt.arguments().isEmpty() || (!opt.mi.equals("prod") && !opt.mi.equals("dev"))) {
        cli.usage()
        System.exit(0)
    }
    if (opt.mi.equals("prod")) {
        mongoDb = "productcatalog_prod"
        mongoServers = [
                [host: "dfwclus5br0vz9.dfw.objectrocket.com", port: "31208"],
                [host: "dfwclus5br1vz9.dfw.objectrocket.com", port: "31208"],
                [host: "dfwclus5br2vz9.dfw.objectrocket.com", port: "31208"]
        ]

    } else {
        mongoDb = "productcatalog_stage"
        mongoServers = [
                [host: "dfwclus0br0vz11.dfw.objectrocket.com", port: "30930"],
                [host: "dfwclus0br1vz11.dfw.objectrocket.com", port: "30930"],
                [host: "dfwclus0br2vz11.dfw.objectrocket.com", port: "30930"]
        ]
    }
    if (opt.eh) {
        esHost = opt.eh
    }
}

/**
 * Deletes the river and catalog indices
 */
void deleteIndices() {
    println "deleting the _river and catalog indices"
    try {
        es.delete(path: '_river')
    } catch (ignored) {
        // delete errors can be ignored
    }
    try {
        es.delete(path: 'catalog')
    } catch (ignored) {
        // delete errors can be ignored
    }
}

/**
 * Create the catalog index in elasticsearch
 */
void createCatalogIndex() {
    println "creating the new catalog index and mapping"
    def resp = es.put(
            path: "/catalog",
            body: getJsonForCatalogIndex(),
            requestContentType: JSON
    )
    assert (resp.status == 200)
}

/**
 * Create the _river index and import the collections
 */
def createCollectionRivers() {
    for (String collection : collections) {
        println "Creating the river for ${collection}"
        def resp = es.put(
                path: "/_river/mongoriver${collection}/_meta",
                body: getJsonForRiver(collection),
                requestContentType: JSON
        )
        assert (resp.status == 201)
    }
}

/**
 * @param mongoCollection we wish to create a river for
 * @return the JSON payload for creating a river on the given collection
 */
String getJsonForRiver(mongoCollection) {
    def builder = new JsonBuilder()
    builder {
        type 'mongodb'
        mongodb {
            servers mongoServers
            options {
                secondary_read_preference 'true'
                drop_collection 'false'
                include_collection 'true'
                import_all_collections 'false'
                skip_initial_import 'false'
            }
            credentials([[db: oplogDb, user: oplogUser, password: oplogPass, auth: mongoAuthDb]])
            db mongoDb
            collection mongoCollection
        }
        index {
            name "catalog"
            type mongoCollection
        }
    }

    return builder.toString()
}

/**
 *
 * @return json definition of the catalog index and an accent free analyzer for the designer.name field
 */
@SuppressWarnings("GroovyAssignabilityCheck")
String getJsonForCatalogIndex() {
    def builder = new JsonBuilder()
    builder {
        settings {
            analysis {
                analyzer {
                    folding {
                        tokenizer "standard"
                        filter "lowercase", "asciifolding"
                    }
                }
            }
        }
        mappings {
            Dress {
                properties {
                    designer {
                        properties {
                            name {
                                type "string"
                                analyzer "folding"
                            }
                        }
                    }
                }
            }
            Accessory {
                properties {
                    designer {
                        properties {
                            name {
                                type "string"
                                analyzer "folding"
                            }
                        }
                    }
                }
            }
            SaleableProduct {
                properties {
                    designer {
                        properties {
                            name {
                                type "string"
                                analyzer "folding"
                            }
                        }
                    }
                }
            }
            Designer {
                properties {
                    name {
                        type "string"
                        analyzer "folding"
                    }
                }
            }
        }
    }
    return builder.toString()
}

