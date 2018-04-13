import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

public class Driver {
	public static void main(String args[]) {
		SparkConf sparkConf = new SparkConf();
		 
		sparkConf.setAppName("boolean search");
		sparkConf.setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		//invertedIndex file
		JavaRDD<String> invertedIndexFile = sc.textFile("lab2_data/part-r-00000").cache();

		Scanner scanner = new Scanner(System.in);
		String line;
		//article files
		JavaPairRDD<String,String> articles = sc.textFile("lab2_data/wiki_*").mapToPair(tuple -> {
			String docline[] = tuple.split(",");

			return new Tuple2<String,String>(docline[0], tuple);
		});
		List<String[]> res = new ArrayList<>();//web infos
		List<String> ids = new ArrayList<>();
		while (!(line = scanner.nextLine()).equals("q")) {
//			JavaPairRDD<String, String> invertedIndex = booleanSearch(invertedIndexFile,line);
			JavaPairRDD<String, String> invertedIndex = booleanSearch(invertedIndexFile,line).reduceByKey(
					new org.apache.spark.api.java.function.Function2<String, String, String>() {
						public String call(String v1, String v2) throws Exception {
						return v1 + " " + v2;
					}
			});

			String keywords = getKeyWords(line);

			JavaPairRDD<String, Tuple2<String, String>> searchedArticles = articles.join(invertedIndex);

			JavaRDD<String[]> resRDD = searchedArticles.map(tuple -> {
				String content = tuple._2._1;
				String info[] = webInfo(content, keywords);

				return info;
			});

//			ids = searchedArticles.keys().collect();
//			res = resRDD.collect();
		}

//		System.out.println(res.size());
//		for(String[] s : res) {
//			System.out.println(s[3]);
//			System.out.println("");
//		}

		sc.close();
	}

	public static String getKeyWords(String searchWords){
		String[] words = toKeyWordsArray(searchWords);
		StringBuilder result = new StringBuilder();
		int index = 0;
		while(index < words.length - 1) {
			if(words[index].equals("and not")) {
				if(words[index + 1].equals("(")) {
					while(!words[++index].equals(")"));
				}
				else {
					index++;
				}
			}
			else if(!words[index].equals("(")&&!words[index].equals(")")&&!words[index].equals("and")&&!words[index].equals("or")) {
				result.append(words[index]);
				result.append(" ");
			}
			index++;
		}
		return result.toString();
	}
	
	
	public static JavaPairRDD<String,String> booleanSearch(JavaRDD<String> invertedIndexRDD,String searchWords) {
		String[] words = toKeyWordsArray(searchWords);
		Stack<JavaPairRDD<String,String>> operands = new Stack();
		Stack<String> operators = new Stack();
		
		for(String word: words) {
			if(word.equals("(")) {
				operators.push(word);
			}
			else if(word.equals("and")||word.equals("or")||word.equals("and not")||word.equals(")")) {
				if(!operators.isEmpty()&&!operators.peek().equals("(")) {
					operands.push(calculator(operands.pop(),operands.pop(),operators.pop()));
				}
				if(word.equals(")")) {
					operators.pop();
				}
				else {
					operators.push(word);
				}
			}
			else if(word.equals("#")) {
				while(!operators.isEmpty()) {
					operands.push(calculator(operands.pop(),operands.pop(),operators.pop()));
				}
			}
			else {
				operands.push(getPairRDD(invertedIndexRDD,word));
			}
			
		}
		System.out.println("finished");
		JavaPairRDD<String,String> top = operands.pop();
		top.foreach(data -> {
			System.out.println("keyword="+data._1() + " data=" + data._2());
		});
		/*
		JavaPairRDD<String,Iterable<String>> tmp = top.groupByKey();
		JavaPairRDD<String,String> result = tmp.mapToPair(tuple -> {
			StringBuilder sb = new StringBuilder();
			Iterator<String> it = tuple._2.iterator();
			while(it.hasNext()) {
				sb.append(it.next()+" ");
			}
			return new Tuple2<String,String>(tuple._1,sb.toString().substring(0,sb.length()-1));
		});

		result.foreach(data -> {
	        System.out.println("docID="+data._1() + " position=" + data._2());
	    });
		*/
		return top;

	}
	
	public static JavaPairRDD<String,String> calculator(JavaPairRDD<String,String> operand1,JavaPairRDD<String,String> operand2, String operator){
		JavaPairRDD<String,String> result = null;
		switch(operator) {
		case "and":
			result = operand2.cogroup(operand1).filter(tuple -> tuple._2._1.iterator().hasNext()&&tuple._2._2.iterator().hasNext())
			.mapToPair(tuple -> {
				StringBuilder sb = new StringBuilder();
				Iterator<String> it = tuple._2._1.iterator();
				while(it.hasNext()) {
					sb.append(it.next()+" ");
				}
				it = tuple._2._2.iterator();
				while(it.hasNext()) {
					sb.append(it.next()+" ");
				}
				return new Tuple2<String,String>(tuple._1,sb.toString().substring(0,sb.length()-1));
			});
			/*new Function<Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>>,Boolean>() {
				@Override  
	            public Boolean call(Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>> s) throws Exception {  
	            		return s._2._1.iterator().hasNext()&&s._2._2.iterator().hasNext();
	            }
			}*/
			break;
		case "or":
			result = operand2.union(operand1);
			break;
		case "and not":
			result = operand2.subtractByKey(operand1);
			break;
		default:
		}
		return result;
	}
	
	public static JavaPairRDD<String,String> getPairRDD(JavaRDD<String> invertedIndexRDD,String keyWord){
		JavaRDD<String> wordLine = invertedIndexRDD.filter(s -> {
			String[] tmp = s.split("\\s+");
            return (tmp[0].equals(keyWord));
		}); 
		/*
		 * new Function<String, Boolean>() {  
            @Override  
            public Boolean call(String s) throws Exception {  
            		String[] tmp = s.split("\\s+");
               return (tmp[0].equals(keyWord));  
            }  
        }*/
		JavaPairRDD<String,String> pairRDD = wordLine.flatMapToPair(s ->{
				ArrayList<Tuple2<String, String>> result = new ArrayList();
            		for(int i = 0; i < s.length(); i++) {
            			if(s.charAt(i) == '(') {
            				int j = i;
            				String docId = "", position = "";
            				while(s.charAt(++j) != ')') {
            					if(s.charAt(j) == 'd') {
            						int k = j+1;
            						while(Character.isDigit(s.charAt(++k)));
            						docId = s.substring(j+2, k);
            						j = k;
            					}
            					else if(s.charAt(j) == 'p') {
            						int k = j+1;
            						while(Character.isDigit(s.charAt(++k)));
            						position = s.substring(j+2, k);
            						j = k - 1;
            					}
            				}
            				result.add(new Tuple2<String,String>(docId,position));
            			}
            		}
            		return result.iterator(); 
		});
		return pairRDD;
	}
	
	public static String[] toKeyWordsArray(String searchWords) {
		searchWords = searchWords.toLowerCase();
		ArrayList<String> result = new ArrayList();
		for(int i = 0; i < searchWords.length(); i++) {
			char c = searchWords.charAt(i);
			if(Character.isLetter(c)) {
				int j = i;
				while(++j < searchWords.length()) {
					if(!Character.isLetter(searchWords.charAt(j))) {
						break;
					}
				}
				String newWord = searchWords.substring(i,j);
				if(newWord.equals("not")) {
					result.remove(result.size()-1);
					result.add("and not");
				}
				else {
					result.add(newWord);
				}
				i = j - 1;
			}
			else if(c == '(') {
				result.add("(");
			}
			else if(c == ')') {
				result.add(")");
			}
		}
		result.add("#");
		return result.toArray(new String[result.size()]);
	}

	public static String[] webInfo(String line, String keywords) {
		String[] docline = line.split(",");
		String docID = docline[0];
		String docURL = docline[1];
		String title = docline[2];
		String content = docline[3];

		String titleSnippet = textSnippet(title, keywords);
		String contentSnippet = textSnippet(content, keywords);

		String info[] = new String[4];
		info[0] = docID;
		info[1] = docURL;
		info[2] = titleSnippet;
		info[3] = contentSnippet;
		return info;

	}


	public static String textSnippet(String content, String keywords) {
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		RAMDirectory ramDirectory = new RAMDirectory();
		IndexWriter indexWriter;
		Document doc = new Document(); // create a new document

		/**
		 * Create a field with term vector enabled
		 */
		FieldType type = new FieldType();
		type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		type.setStored(true);
		type.setStoreTermVectors(true);
		type.setTokenized(true);
		type.setStoreTermVectorOffsets(true);

		Field f = new Field("content", content, type);
		doc.add(f);

		try {
			indexWriter = new IndexWriter(ramDirectory, config);
			indexWriter.addDocument(doc);
			indexWriter.close();

			IndexReader idxReader = DirectoryReader.open(ramDirectory);
			IndexSearcher idxSearcher = new IndexSearcher(idxReader);
			Query queryToSearch = new QueryParser("content", analyzer).parse(keywords);
			TopDocs hits = idxSearcher
					.search(queryToSearch, idxReader.maxDoc());
			SimpleHTMLFormatter htmlFormatter = new SimpleHTMLFormatter();
			Highlighter highlighter = new Highlighter(htmlFormatter,
					new QueryScorer(queryToSearch));

//			System.out.println("reader maxDoc is " + idxReader.maxDoc());
//			System.out.println("scoreDoc size: " + hits.scoreDocs.length);
			for (int i = 0; i < hits.totalHits; i++) {
				int id = hits.scoreDocs[i].doc;
				Document docHit = idxSearcher.doc(id);
				String text = docHit.get("content");
				TokenStream tokenStream = TokenSources.getAnyTokenStream(idxReader, id, "content", analyzer);
				TextFragment[] frag = highlighter.getBestTextFragments(tokenStream, text, false, 4);
				for (int j = 0; j < frag.length; j++) {
					if ((frag[j] != null) && (frag[j].getScore() > 0)) {
						//System.out.println((frag[j].toString()));
						return frag[j].toString();
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch(ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTokenOffsetsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
}

/*
 * new PairFlatMapFunction<String,String,String>(){
			@Override  
            public Iterator<Tuple2<String, String>> call(String s) throws Exception {  
				ArrayList<Tuple2<String, String>> result = new ArrayList();
            		for(int i = 0; i < s.length(); i++) {
            			if(s.charAt(i) == '(') {
            				int j = i;
            				String docId = "", position = "";
            				while(s.charAt(++j) != ')') {
            					if(s.charAt(j) == 'd') {
            						int k = j+1;
            						while(Character.isDigit(s.charAt(++k)));
            						docId = s.substring(j+2, k);
            						j = k;
            					}
            					else if(s.charAt(j) == 'p') {
            						int k = j+1;
            						while(Character.isDigit(s.charAt(++k)));
            						position = s.substring(j+2, k);
            						j = k - 1;
            					}
            				}
            				result.add(new Tuple2<String,String>(docId,position));
            			}
            		}
            		return result.iterator();
            } 
		}*/
