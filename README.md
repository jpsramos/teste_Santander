# teste_Santander

Teste basedo em um arquivo de log. O arquivo de log segue o padrão **Web Server Access Log**, e cada linha representa uma requisição HTTP.

No seguinte formato, abaixo um print breve de sua estrutura:

![image](https://github.com/user-attachments/assets/10e73cbf-ccb2-441c-9b95-99328886a676)

O teste deve cobrir respostas utilizando pyspark para os seguintes desafios:

### **Desafio:**
1. **Identifique as 10 maiores origens de acesso (Client IP) por quantidade de acessos.**
2. **Liste os 6 endpoints mais acessados, desconsiderando aqueles que representam arquivos.**
3. **Qual a quantidade de Client IPs distintos?**
4. **Quantos dias de dados estão representados no arquivo?**
5. **Com base no tamanho (em bytes) do conteúdo das respostas, faça a seguinte análise:**
   - O volume total de dados retornado.
   - O maior volume de dados em uma única resposta.
   - O menor volume de dados em uma única resposta.
   - O volume médio de dados retornado.
   - *Dica:* Considere como os dados podem ser categorizados por tipo de resposta para realizar essas análises.
6. **Qual o dia da semana com o maior número de erros do tipo "HTTP Client Error"?**
---

