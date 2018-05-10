from gensim.models import word2vec
import logging
import os
import data

# this class use to make the iter
class MySentences(object):

    def __init__(self, dirname):
        self.dirname = dirname

    def get_low_qu_list(self):
        all_data = data.read_corpus(self.dirname)
        word2id = {}
        for sent_, tag_ in all_data:
            for word in sent_:
                if word.isdigit():
                    word = '<NUM>'
                elif ('\u0041' <= word <= '\u005a') or ('\u0061' <= word <= '\u007a'):
                    word = '<ENG>'
                if word not in word2id:
                    word2id[word] = [len(word2id) + 1, 1]
                else:
                    word2id[word][1] += 1
        low_freq_words = []
        for word, [word_id, word_freq] in word2id.items():
            if word_freq < 3 and word != '<NUM>' and word != '<ENG>':
                low_freq_words.append(word)
        return low_freq_words

    def __iter__(self):

        low_list = self.get_low_qu_list()
        with open(self.dirname, encoding='utf-8') as fr:
            lines = fr.readlines()
        sent_, tag_ = [], []
        for line in lines:
            if line != '\n':
                [char, label] = line.strip().split()

                if char.isdigit():
                    char = '<NUM>'
                elif ('\u0041' <= char <= '\u005a') or ('\u0061' <= char <= '\u007a'):
                    char = '<ENG>'
                elif char in low_list:
                    char = '<UNK>'

                sent_.append(char)
                tag_.append(label)
            else:
                yield sent_
                sent_, tag_ = [], []


logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

sentences = MySentences('data_path/train_data')
model = word2vec.Word2Vec(sentences, min_count=2, size=300)
#
model.save('relative_data/embedding_model/embedding_201804262.model')


#model = word2vec.Word2Vec.load('relative_data/embedding_model/embedding_201804251.model')

data_list = ['我', '是', '一', '个', '学', '生', '南', '北', '广', '省', '市']
vertor_list = []
for item in data_list:
    print(model[item])
    vertor_list.append(model[item].tolist())

print(vertor_list)

#print(model['囧'])
try:
    print(model['囧'])
except:
    print(model['<UNK>'])


y = model.most_similar(['党'] , topn=20)
for item in y:
    print(item)


print('.................................................')
y = model.most_similar(['市'] , topn=20)
for item in y:
    print(item)

