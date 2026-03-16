# must：[] 各条件之间是and的关系
body = {
    "query": {
        "bool": {
            'must': [
                {
                    "term": {
                        'ziduan1.keyword': '我爱你中国'}
                },
                {
                    'terms': {
                        'ziduan2': ['I love', 'China']}
                }
            ]
        }
    }
}

# should: [] 各条件之间是or的关系
body = {
    "query": {
        "bool": {
            'should': [{
                "term": {
                    'ziduan1.keyword': '我爱你中国'}},
                {
                    'terms': {
                        'ziduan2': ['I love', 'China']}}]
        }
    }
}

# must_not：[]各条件都不满足
body = {
    "query": {
        "bool": {
            'must_not': [{
                "term": {
                    'ziduan1.keyword': '我爱你中国'}},
                {
                    'terms': {
                        'ziduan2': ['I love', 'China']}}]
        }
    }
}

# bool嵌套bool
# ziduan1、ziduan2条件必须满足的前提下，ziduan3、ziduan4满足一个即可
body = {

    "query": {
        "bool": {
            "must": [{
                "term": {
                    "ziduan1": "China"}},  # 多个条件并列 ，注意：must后面是[{}, {}],[]里面的每个条件外面有个{}
                {
                    "term": {
                        "ziduan2.keyword": '我爱你中国'}},
                {
                    'bool': {

                        'should': [
                            {
                                'term': {
                                    'ziduan3': 'Love'}},
                            {
                                'term': {
                                    'ziduan4': 'Like'}}
                        ]
                    }}
            ]
        }
    }
}