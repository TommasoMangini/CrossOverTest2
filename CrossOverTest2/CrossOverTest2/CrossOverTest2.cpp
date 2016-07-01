// CrossOverTest2.cpp : definisce il punto di ingresso dell'applicazione console.
//

//	Compilare in x64 e release a causa del c++ MySQL connector
//	File mysqlcppconn.dll presente nella cartella x64\Release
//	Richiede l'installazione delle librerie boost

#include "stdafx.h"



#include <cpprest/http_client.h>
#include <cpprest/filestream.h>
#include <cpprest/json.h>

#include "cpprest/containerstream.h"

/* Standard C++ includes */
#include <stdlib.h>
//#include <iostream> 

/*
Include directly the different
headers from cppconn/ and mysql_driver.h + mysql_util.h
(and mysql_connection.h). This will reduce your build time!
*/
#include "mysql_connection.h"

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>


using namespace utility;
using namespace web;
using namespace web::http;
using namespace web::http::client;
using namespace concurrency::streams;

using namespace std;


#include <codecvt>
#include <string>

// convert wstring to UTF-8 string
std::string wstring_to_utf8(const std::wstring& str)
{
	std::wstring_convert<std::codecvt_utf8<wchar_t>> myconv;
	return myconv.to_bytes(str);
}

#include "cpprest/filestream.h"
#include "cpprest/containerstream.h"
#include "cpprest/producerconsumerstream.h"

using namespace utility;
using namespace concurrency::streams;

/// <summary>
/// A convenient helper function to loop asychronously until a condition is met.
/// </summary>
pplx::task<bool> _do_while_iteration(std::function<pplx::task<bool>(void)> func)
{
	pplx::task_completion_event<bool> ev;
	func().then([=](bool guard)
	{
		ev.set(guard);
	});
	return pplx::create_task(ev);
}
pplx::task<bool> _do_while_impl(std::function<pplx::task<bool>(void)> func)
{
	return _do_while_iteration(func).then([=](bool guard) -> pplx::task<bool>
	{
		if (guard)
		{
			return ::_do_while_impl(func);
		}
		else
		{
			return pplx::task_from_result(false);
		}
	});
}
pplx::task<void> do_while(std::function<pplx::task<bool>(void)> func)
{
	return _do_while_impl(func).then([](bool) {});
}

/// <summary>
/// Structure used to store individual line results.
/// </summary>
typedef std::vector<std::string> matched_lines;
namespace Concurrency {
	namespace streams {
		/// <summary>
		/// Parser implementation for 'matched_lines' type.
		/// </summary>
		template <typename CharType>
		class type_parser<CharType, matched_lines>
		{
		public:
			static pplx::task<matched_lines> parse(streambuf<CharType> buffer)
			{
				basic_istream<CharType> in(buffer);
				auto lines = std::make_shared<matched_lines>();
				return do_while([=]()
				{
					container_buffer<std::string> line;
					return in.read_line(line).then([=](const size_t bytesRead)
					{
						if (bytesRead == 0 && in.is_eof())
						{
							return false;
						}
						else
						{
							lines->push_back(std::move(line.collection()));
							return true;
						}
					});
				}).then([=]()
				{
					return matched_lines(std::move(*lines));
				});
			}
		};
	}
}
/// <summary>
/// Function to create in data from a file and search for a given string writing all lines containing the string to memory_buffer.
/// </summary>
static pplx::task<void> find_matches_in_file(const string_t &fileName, const std::string &searchString, concurrency::streams::basic_ostream<char> results)
{
	return file_stream<char>::open_istream(fileName).then([=](concurrency::streams::basic_istream<char> inFile)
	{
		auto lineNumber = std::make_shared<int>(1);
		return ::do_while([=]()
		{
			container_buffer<std::string> inLine;
			return inFile.read_line(inLine).then([=](size_t bytesRead)
			{
				if (bytesRead == 0 && inFile.is_eof())
				{
					return pplx::task_from_result(false);
				}

				else if (inLine.collection().find(searchString) != std::string::npos)
				{
					results.print("line ");
					results.print((*lineNumber)++);
					return results.print(":").then([=](size_t)
					{
						container_buffer<std::string> outLine(std::move(inLine.collection()));
						return results.write(outLine, outLine.collection().size());
					}).then([=](size_t)
					{
						return results.print("\r\n");
					}).then([=](size_t)
					{
						return true;
					});
				}

				else
				{
					++(*lineNumber);
					return pplx::task_from_result(true);
				}
			});
		}).then([=]()
		{
			// Close the file and results stream.
			return inFile.close() && results.close();
		});
	});
}

/// <summary>
/// Function to write out results from matched_lines type to file
/// </summary>
static pplx::task<void> write_matches_to_file(const string_t &fileName, matched_lines results, sql::Connection *con)
{
	// Create a shared pointer to the matched_lines structure to copying repeatedly.
	auto sharedResults = std::make_shared<matched_lines>(std::move(results));	

	return file_stream<char>::open_ostream(fileName, std::ios::trunc).then([=](concurrency::streams::basic_ostream<char> outFile)
	{
		auto currentIndex = std::make_shared<size_t>(0);
		
		auto i = std::make_shared<int>(0);

		return ::do_while([=]()
		{

			if (*currentIndex >= sharedResults->size())
			{
				return pplx::task_from_result(false);
			}

			container_buffer<std::string> lineData((*sharedResults)[(*currentIndex)++]);
			outFile.write(lineData, lineData.collection().size());


			sql::PreparedStatement *pstmt;
			/* '?' is the supported placeholder syntax */
			pstmt = con->prepareStatement("INSERT INTO TestColumn1(id, id2) VALUES (?, ?)");
			try
			{
				pstmt->setInt(1, *i);				
				
				std::string s = lineData.collection();

				pstmt->setString(2, s);
				pstmt->executeUpdate();
			}
			catch (sql::SQLException &e)
			{
				cout << "# ERR: SQLException in " << __FILE__;
				cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
				cout << "# ERR: " << e.what();
				cout << " (MySQL error code: " << e.getErrorCode();
				cout << ", SQLState: " << e.getSQLState() << " )" << endl;
			}
			delete pstmt;

			(*i)++;

			return outFile.print("\r\n").then([](size_t)
			{
				return true;
			});
		}).then([=]()
		{
			return outFile.close();
		});
	});
}

#ifdef _WIN32
int wmain(int argc, wchar_t *args[])
#else
int main(int argc, char *args[])
#endif
{
	if (argc != 4)
	{
		printf("Usage: SearchFile.exe input_file search_string output_file\n");
		return -1;
	}

	sql::Driver *driver;
	sql::Connection *con;
	sql::Statement *stmt;
	sql::ResultSet *res;
	sql::PreparedStatement *pstmt;

	try {			
		/* Create a connection */
		driver = get_driver_instance();
		con = driver->connect("tcp://127.0.0.1:3306", "root", "1234");
		/* Connect to the MySQL test database */
		con->setSchema("mysql");
	
		stmt = con->createStatement();
	
		//	Warning: it deletes the database test
		stmt->execute("DROP DATABASE IF EXISTS test");
		stmt->execute("CREATE DATABASE test");
		stmt->execute("USE test");
	
		stmt->execute("CREATE TABLE TestColumn1(id INT, id2 varchar(100))");
		delete stmt;
	
			
	
		/* Select in ascending order */
		pstmt = con->prepareStatement("SELECT id FROM TestColumn1 ORDER BY id ASC");
		res = pstmt->executeQuery();
	
		/* Fetch in reverse = descending order! */
		res->afterLast();
		while (res->previous())
			cout << "\t... MySQL counts: " << res->getInt("id") << endl;
		delete res;
	
		delete pstmt;		
	
	}
	catch (sql::SQLException &e) {
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
	
	cout << endl;

	const string_t inFileName = args[1];
	const std::string searchString = utility::conversions::to_utf8string(args[2]);
	const string_t outFileName = args[3];
	producer_consumer_buffer<char> lineResultsBuffer;

	// Find all matches in file.
	concurrency::streams::

		basic_ostream<char> outLineResults(lineResultsBuffer);
	find_matches_in_file(inFileName, searchString, outLineResults)

		// Write matches into custom data structure.
		.then([&]()
	{
		concurrency::streams::

		basic_istream<char> inLineResults(lineResultsBuffer);
		return inLineResults.extract<matched_lines>();
	})

		// Write out stored match data to a new file.
		.then([&](matched_lines lines)
	{
		return write_matches_to_file(outFileName, std::move(lines), con);
	})

		// Wait for everything to complete.
		.wait();

	delete con;

	return EXIT_SUCCESS;
}


