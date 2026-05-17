using BsAutoRec.ViewModels;
using System.Windows;

namespace BsAutoRec
{
	public partial class MainWindow : Window
	{
		public MainWindow(MainViewModel viewModel)
		{
			InitializeComponent();
			DataContext = viewModel;
		}
	}
}
