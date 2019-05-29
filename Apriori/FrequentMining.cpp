#include<iostream>
#include<map>
#include<vector>
#include<string>
#include<fstream>
#include<set>
#include<algorithm>
#include<list>
#include <iterator>
#include<sstream>
#include<future>
#include<thread>
using namespace std;
using Dataset=vector<list<string>>;
using FrequentItem=vector<set<string>>;
const int linenumber = 1000;
size_t threadnumber = 4;
//读取数据
Dataset readlines(string filename) {
	ifstream file(filename);
	string lines;
	Dataset data(linenumber);
	int i = 0;
	while (getline(file, lines)) {
		istringstream iss(lines);
		list<string>result;
		for (string item; getline(iss, item, ' ');) {
			if (!item.empty())
				result.push_back(item);
		}
		data[i++] = result;
	}
	return data;
}
//统计一项集
FrequentItem initFrequent(const Dataset& dataset,const int support) {
	map<string, int>m;
	FrequentItem item;
	for (auto v : dataset) {
		for (auto key: v) {
			m[key]++;
		}
	}
	for (auto p : m) {
		if (p.second > support)
			item.push_back(set<string>({ p.first }));
	}
	return item;
}
//Apriori算法
FrequentItem Apriori(const Dataset& dataset,FrequentItem item) {
	FrequentItem results;
	int support = int(dataset.size() * 0.1);
	//统计一项集
	if (item.empty())
		item = initFrequent(dataset, support);
	results.insert(results.end(), item.begin(), item.end());
	map<set<string>, int>ms;
	for (int i = 0; i < item.size() - 1; i++) {
		for (int j = i + 1; j < item.size(); j++) {
			set<string> s;
			//将两个项集进行连接并判断是否是候选的频繁项集
			set_union(item[i].begin(), item[i].end(), item[j].begin(), item[j].end(), inserter(s,s.begin()));
			if (s.size() == item[i].size() + 1) {
				vector<string>subv(s.begin(), s.end());
				bool all = true;
				for (int k = 0; k < subv.size(); k++) {
					swap(subv[k], subv[0]);
					if (find(item.begin(), item.end(), set<string>(subv.begin() + 1, subv.end()))==item.end()) {
						all = false;
						break;
					}
				}
				//如果是候选的频繁项集,则统计项的个数
				if (all) {
					for (auto data : dataset) {
						if (all_of(s.begin(), s.end(),
							[data](string i) {return find(data.begin(), data.end(), i) != data.end(); })
							) {
							ms[s]++;
						}
					}
				}
			}
		}
	}
	item.clear();
	for (auto p : ms) {
		if (p.second > support)
			item.push_back(p.first);
	}
	//当当前的频繁项集还有多个项集时,需要继续进行挖掘
	if (item.size() > 1) {
		auto result = Apriori(dataset, item);
		results.insert(results.end(), result.begin(), result.end());
	}
	else {
		results.push_back(item.front());
	}
	return results;
}
//统计频繁项中某一项的个数
int Counts(const Dataset& data, set<string>ss,int support) {
	int num = 0;
	for (auto rr : data) {
		if (all_of(ss.begin(), ss.end(),
			[rr](string i) {return find(rr.begin(), rr.end(), i) != rr.end(); })
			) {
			num++;
		}
	}
	return num;
}

FrequentItem Process(string filename) {
	if (linenumber < 10000) {
		threadnumber = 1;
	}
	size_t thread_lines = int(linenumber / threadnumber);
	size_t num = 0;
	FrequentItem Item;
	set<set<string>>ss;
	Dataset data = readlines(filename);
	vector<future<FrequentItem>>vs(threadnumber);
	//使用多线程加速
	while (num < threadnumber - 1) {
		Dataset subDataSet(data.begin()+num*thread_lines,data.begin()+(num+1)*thread_lines);
		FrequentItem Freitem;
		vs[num] = async(launch::async,Apriori, subDataSet, Freitem);
		num++;
	}
	Dataset subDataSet(data.begin()+num*thread_lines,data.end());
	FrequentItem freitem;
	vs[num] = async(launch::async,Apriori, subDataSet, freitem);
	for (int i = 0; i < threadnumber; i++) {
		auto result = vs[i].get();
		ss.insert(result.begin(), result.end());
	}
	vector<future<int>>re(ss.size());
	num = 0;
	int support = int(data.size() * 0.1);
	vector<set<string>>vsit(ss.begin(), ss.end());
	for (int i = 0; i < vsit.size(); i++) {
		re[i] = async(launch::async, Counts, data, vsit[i], support);
	}
	
	for (int i = 0; i < vsit.size(); i++) {
		auto result = re[i].get();
		if (result > support) {
			for (auto s : vsit[i]) {
				cout << s << ' ';
			}
			cout << '\t' << result<<'\n';
			Item.push_back(vsit[i]);
		}
	}
	return Item;
}

int main() {
	string file = R"(C:\Users\wangl\Downloads\k.txt)";
	auto result = Process(file);
}