/**
 * - Ler a panilha;
 * - Codificar as colunas;
 * - Contar a quantidade de registros por coluna;
 * - Exibir o resultado final.
 * 27 colunas
 */

#include <iostream>
#include <sstream>
#include <fstream>
#include <strstream>
#include <omp.h>
#include <string.h>
#include <stdbool.h>
#include <iterator>
#include <map>
#include <vector>
#include <windows.h>

#define CAMINHO_ARQUIVO_CSV "dataset_00_1000.csv"
#define DELIMITER ","

using namespace std;

int main(int argc, char const* argv[]) {
    // instanciando todas variaveis necessarias
    std::string linha;
    std::string celula;
    map<int, std::string> colunas;
    int idColuna = 0, idValorColuna, indiceLinha = 0, quantidadeLinha, quantidadeColuna;
    std::vector<std::map<std::string, int>> valoresColunasId(3000);
    int ocorrenciaValoresColunas[50][1000] = {};
    int planilha[50][2300] = {};
    int idsValoresColuna[3000] = { 0 };

    // Abre o arquivo que contem o csv
    std::ifstream arquivo;
    arquivo.open(CAMINHO_ARQUIVO_CSV);

    if (!arquivo.good()) {
        cerr << "Erro: Arquivo " << CAMINHO_ARQUIVO_CSV << " não pode ser aberto" << endl;
        exit(1);
    }

    cout << "Criando subpasta resultado onde ira armazenar todos os cvs com os resultados\n";
    system("mkdir resultado");

    std::ofstream myfile;

    myfile.open("resultado/dataset_codificado.csv");

    // salvando em um map todas as colunas e atribuindo um id para cada coluna
    std::getline(arquivo, linha);
    std::istringstream linhaCsv(linha);
    while (std::getline(linhaCsv, celula, ','))
    {
        if (linhaCsv.tellg() == -1) {
            myfile << celula << "\n";
        }
        else {
            myfile << celula << ",";
        }

        colunas.insert(pair<int, std::string>(idColuna++, celula));
    }

    quantidadeColuna = idColuna;

    string linhaCodificada;

    while (std::getline(arquivo, linha)) {
        std::istringstream linhaCsv(linha);
        idColuna = 0;
        linhaCodificada = "";

                
        for (idColuna = 0; idColuna < quantidadeColuna; idColuna++)
        {
            std::getline(linhaCsv, celula, ',');

            map<std::string, int>::const_iterator pos = valoresColunasId[idColuna].find(celula);

            if (pos == valoresColunasId[idColuna].end()) {
                valoresColunasId[idColuna].insert(pair<std::string, int>(celula, idsValoresColuna[idColuna]));
                idValorColuna = idsValoresColuna[idColuna]++;
            }
            else {
                idValorColuna = pos->second;
            }

            ocorrenciaValoresColunas[idColuna][idValorColuna]++;
            planilha[idColuna][indiceLinha] = idValorColuna;

            linhaCodificada += std::to_string(idValorColuna);
            if (idColuna < quantidadeColuna - 1) {
                linhaCodificada += ",";
            }
        }
        myfile << linhaCodificada << "\n";

        indiceLinha++;
    }

    quantidadeLinha = indiceLinha;

    myfile.close();
    arquivo.close();

    map<int, std::string>::iterator colunaIterator;

    for (colunaIterator = colunas.begin(), idColuna = 0; colunaIterator != colunas.end(); ++colunaIterator, idColuna++) {
        std::ofstream myfile;

        myfile.open("resultado/ocorrencias_coluna_" + colunaIterator->second + ".csv");
        myfile << colunaIterator->second << ",OCOR\n";

        map<std::string, int>::iterator valoresColunaIterator;

        for (valoresColunaIterator = valoresColunasId[idColuna].begin(), idValorColuna = 0; valoresColunaIterator != valoresColunasId[idColuna].end(); ++valoresColunaIterator, idValorColuna++) {
            myfile << valoresColunaIterator->first << "," << ocorrenciaValoresColunas[idColuna][idValorColuna] << "\n";
        }

        myfile.close();
    }

    cout << "Finalizado voce pode encontrar os csv dentro da pasta resultado\n";

    return 0;
}