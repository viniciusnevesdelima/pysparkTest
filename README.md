# pysparkTest

# Questões

 <b>1. Qual o objetivo do comando cache em Spark?</b></p>
      <div>R: Salvam segmentos do resultados em memória volátil e podem ser reutilizados em outros estágios. Com os resultados       próvisorios como RDD's permanecem em RAM ou salvos/replicados em HD</div></p>
<b>2. O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em &nbsp;MapReduce.Por quê?</b></p>
<div>
R:O MapReduce é uma ótima ferramenta para execução para cálculos sofisticados,mas não operativo para várias execuções em fila.Isto acontece devido aos fluxos de processamento são divididos em apenas uma fase Map e uma fase Reduce e, &nbsp;desse modo é necessário a utilização do padrão MapReduce. A saida do processo são armazenados no hdfs antes de cada &nbsp;passo. Com isso esta abordagem tende a lentidão devido à replicação. Já o Spark estende o MapReduce , abstendo-se &nbsp;de dividir/mover os dados durante qualquer processamento. O desempenho é quase 100 x maior que outras técnicas do &nbsp;mundo Big data, justamente pelo aramzenamento de infomarção em memporia.</p>
</div>
<b>3. Qual é a função do SparkContext ?</b></p>
<div>
R:Responsável por inicializar o driver do Spark e acessar o Spark Cluster juntamente com um gerenciador de recursos(yarn/mesos).Antes de cria-lo deve-se criar o SparkConf.
</div></p>
<b>4. Explique com suas palavras o que é Resilient Distributed Datasets (RDD).</b></p>
<div>
R: O RDD parece como uma tabela e/ou documento que pode guardar qualquer tipo de dado.O Spark distribui o armazenamento de dados dos RDD's em diversas partições. 
</div></p>
<b>5. GroupByKey é menos eficiente que reduceByKey em grandes dataset.Por quê?</b></p>
<div>
R: De acordo com a documentação do código, a groupByKeyoperação agrega os valores de cada chave no RDD em uma única sequência. Também permite controlar o particionamento dos pares de chave-valor resultante RDD passando pelo Partitioner.
Esta operação pode ser muito cara. Se você estiver agrupando para executar uma agregação (como uma soma ou média) sobre cada chave, usar aggregateByKey ou reduceByKey fornecerá um desempenho muito melhor.
</div>
