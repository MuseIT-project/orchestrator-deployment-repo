import json
import requests

with open('foundkeys_missing_indented.json', 'r') as f:
    foundkeys_missing = json.load(f)

missinglist = []

foundlist = []

for item in foundkeys_missing:
    url = item['image']
    response = requests.get(url)
    if response.status_code == 404:
        missinglist.append(url)
    else:
        foundlist.append(url)

print('Missing URLs:' + str(missinglist))
print('Found URLs:' + str(foundlist))

missing_urls = [
    'https://uploads3.wikiart.org/images/agnolo-bronzino/cosimo-de-medici.jpg!Large.jpg', 
    'https://uploads6.wikiart.org/images/tamara-de-lempicka/perspective-1923.jpg!Large.jpg', 
    'https://uploads4.wikiart.org/images/tamara-de-lempicka/maternity-1928.jpg!Large.jpg', 
    'https://uploads6.wikiart.org/images/tamara-de-lempicka/portrait-of-pierre-de-montaut-1931.jpg!Large.jpg', 
    'https://uploads3.wikiart.org/00452/images/tamara-de-lempicka/1946amethyst.jpg!Large.jpg', 
    'https://uploads0.wikiart.org/images/tamara-de-lempicka/bowl-of-grapes-1949.jpg!Large.jpg', 
    'https://uploads1.wikiart.org/images/paolo-uccello/crucifixion-1430.jpg!Large.jpg', 
    'https://uploads8.wikiart.org/images/paolo-uccello/a-young-lady-of-fashion.jpg!Large.jpg', 
    'https://uploads5.wikiart.org/images/joaquin-sorolla/hall-of-the-ambassadors-alhambra-granada-1909.jpg!Large.jpg', 
    'https://uploads7.wikiart.org/images/pablo-picasso/seated-woman-olga-1920.jpg!Large.jpg', 
    'https://uploads5.wikiart.org/images/pablo-picasso/kallan-1923.jpg!Large.jpg', 
    'https://uploads6.wikiart.org/images/pablo-picasso/girl-in-front-of-mirror-1932-1.jpg!Large.jpg', 
    'https://uploads7.wikiart.org/00136/images/henri-matisse/the-sea-seen-from-collioure-1906.jpg!Large.jpg', 
    'https://uploads1.wikiart.org/images/henri-matisse/polynesia-the-sky-1946(1).jpg!Large.jpg', 
    'https://uploads6.wikiart.org/00432/images/andy-warhol/krygier6-18-5-2.jpg!Large.jpg', 
    'https://uploads6.wikiart.org/00428/images/andy-warhol/andy-warhol-do-it-yourself-sailboats-1962-awf-300dpi.jpg!Large.jpg', 
    'https://uploads1.wikiart.org/images/andy-warhol/coca-cola.jpg!Large.jpg', 
    'https://uploads8.wikiart.org/images/andy-warhol/flowers.jpg!Large.jpg', 
    'https://uploads1.wikiart.org/images/andy-warhol/ingrid-bergman-as-herself-1.jpg!Large.jpg', 
    'https://uploads6.wikiart.org/images/andy-warhol/ingrid-bergman-with-hat.jpg!Large.jpg', 
    'https://uploads6.wikiart.org/images/andy-warhol/bunny-multiple.jpg!Large.jpg', 
    'https://uploads5.wikiart.org/images/andy-warhol/queen-beatrix-of-the-netherlands-from-reigning-queens-1985.jpg!Large.jpg', 
    'https://uploads7.wikiart.org/images/mary-cassatt/pattycake-1897.jpg!Large.jpg', 
    'https://uploads2.wikiart.org/images/mary-cassatt/women-admiring-a-child-1897.jpg!Large.jpg', 
    'https://uploads2.wikiart.org/images/john-william-waterhouse/the-annunciation.jpg!Large.jpg', 
    'https://uploads0.wikiart.org/images/andy-warhol/vesuvius.jpg!Large.jpg', 
    'https://uploads6.wikiart.org/images/andy-warhol/cow-3.jpg!Large.jpg', 
    'https://uploads2.wikiart.org/images/andy-warhol/volkswagen.jpg!Large.jpg'
    ]
        
