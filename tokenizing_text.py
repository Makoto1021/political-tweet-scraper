
import MeCab
tagger = MeCab.Tagger()

def tokenize_ja(text, lower):
    node = tagger.parseToNode(str(text))
    while node:
        if lower and node.feature.split(',')[0] in ["名詞","形容詞"]:
            yield node.surface.lower()
        node = node.next

def tokenize(content, token_min_len, token_max_len, lower):
    return [
        str(token) for token in tokenize_ja(content, lower)
        if token_min_len <= len(token) <= token_max_len and not token.startswith('_')
        ]