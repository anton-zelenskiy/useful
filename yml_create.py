from django.utils import timezone
import xml.etree.ElementTree as xml
from project.apps.proposals.elasticsearch.utils import search
from project.apps.proposals.models import ProposalAvailabilityEnum


def get_proposal_ids():
    body = {
        'size': 5000,
        'query': {
            'match_all': {}
        },
        "sort": [
            {
                "_meta.views": {
                    "order": "desc"
                }
            }
        ],
    }

    result = search(body)
    return [i['_source']['id'] for i in result['hits']['hits']]


def create_yml():
    yml_catalog = xml.Element('yml_catalog')
    yml_catalog.set('date', timezone.now().strftime('%Y-%m-%d %H:%M'))

    shop = xml.Element('shop')
    yml_catalog.append(shop)

    categories = xml.Element('categories')
    shop.append(categories)

    offers = xml.Element('offers')
    shop.append(offers)

    shop_name = xml.SubElement(shop, 'name')
    shop_name.text = 'Supl.biz'

    shop_company = xml.SubElement(shop, 'company')
    shop_company.text = 'Supl.biz'

    shop_url = xml.SubElement(shop, 'url')
    shop_url.text = 'https://supl.biz'

    offers_list, category_ids = create_offers(offers)

    # for offer in offers_list:
    #     offers.append(offer)

    categories_list = create_categories(categories, category_ids)
    # for category in categories_list:
    #     categories.append(category)

    tree = xml.ElementTree(yml_catalog)

    with open('/tmp/yml_catalog.xml', 'wb') as file:
        tree.write(file)


def create_offers(parent_element: xml.Element):
    result = []
    categories = set()

    ids = get_proposal_ids()
    print('>len', len(ids))
    proposals = Proposal.objects.filter(
        id__in=get_proposal_ids(),
        categories__isnull=False,
        image__isnull=False,
    ).distinct()

    print(f'COUNT', proposals.count())

    for proposal in proposals:
        offer = xml.SubElement(parent_element, 'offer')
        offer.set('id', str(proposal.id))
        available = 'true' if proposal.availability in (
            ProposalAvailabilityEnum.AVAILABLE,
            ProposalAvailabilityEnum.AVAILABLE_TO_ORDER
        ) else 'false'
        offer.set('available', available)

        offer_url = xml.SubElement(offer, 'url')
        offer_url.text = f'https://supl.biz/{proposal.slug}-p{proposal.id}'

        offer_currency_id = xml.SubElement(offer, 'currencyId')
        offer_currency_id.text = proposal.currency

        category_id = proposal.categories.first().id
        categories.add(category_id)
        offer_category_id = xml.SubElement(offer, 'categoryId')
        offer_category_id.text = str(category_id)

        picture = xml.SubElement(offer, 'picture')
        picture.text = proposal.image.image.url

        offer_name = xml.SubElement(offer, 'name')
        offer_name.text = proposal.title

        description = xml.SubElement(offer, 'description')
        description.text = proposal.title

        if proposal.price:
            offer_price = xml.SubElement(offer, 'price')
            offer_price.text = str(proposal.price)

        if proposal.old_price:
            offer_old_price = xml.SubElement(offer, 'old_price')
            offer_old_price.text = str(proposal.old_price)

        if proposal.price_details:
            sales_notes = xml.SubElement(offer, 'sales_notes')
            sales_notes.text = proposal.price_details

        result.append(offer)

    return result, categories


def create_categories(parent_element: xml.Element, categories: set):
    categories = Category.objects.filter(id__in=categories)

    result = []
    for cat in categories:
        category = xml.SubElement(parent_element, 'category')
        category.set('id', str(cat.id))

        name = xml.SubElement(category, 'name')
        name.text = cat.name

        result.append(category)

    return result
