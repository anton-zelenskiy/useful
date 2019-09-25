def create_new_index():
    from project.apps.index.es.management import EsFactory
    from project.apps.proposals.elastic_search.proposal import EsProposalContainer

    es_container = EsProposalContainer()

    new_es_container = es_container.__class__()
    new_es_container.index = f'{es_container.index}_v2'

    EsFactory(es_container=new_es_container).create_index()


def reindex():
    """Переиндексация  с изменением маппинга для нового индекса. """
    from project.apps.index.es.connect import EsConnector
    from elasticsearch.helpers import reindex

    client = EsConnector.get_connection()
    res = reindex(
        client=client,
        source_index='proposals',
        target_index='proposals_v2',
    )

    return res


def switch_aliases():
    """
POST /_aliases
{
    "actions" : [
        { "remove" : { "index" : "proposals_guol3rqcfs", "alias" : "proposals" } },
        { "add" : { "index" : "proposals_v2", "alias" : "proposals" } }
    ]
}

    """
